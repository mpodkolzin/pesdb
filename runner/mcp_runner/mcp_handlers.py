"""
MCP server request handlers.

Handles MCP tool execution, tool listing, and resource listing.
"""

import sys
import traceback
import subprocess
from contextlib import suppress
from paver.tasks import environment

from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio

from mcp_runner.constants import *
from mcp_runner.task_resolver import *
from mcp_runner.session_manager import *
from mcp_runner.process_executor import *

# Create MCP server instance
server = Server("runner-mcp")


@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict | None
) -> list[types.TextContent]:
    """
    Handle tool execution requests.
    - Special tool: restart_environment (stops interactive sessions and container)
    - Interactive tools: names starting with "interactive-" act as a single 'interact' call.
      They auto-start if not already running, honor mcp_async_requires and mcp_prompt_pattern,
      and then perform interaction (send input / wait).
    - All other tools are blocking calls.
    """
    base_name = name
    try:
        # Handle environment restart tool explicitly
        if base_name == "restart_environment":
            outputs = await stop_interactive_environment(restart=True)
            return format_state_as_text({"status": "restarted", "output": outputs}, base_name)

        # Interactive tool: single 'interact' entry that auto-starts when needed
        if base_name.startswith("interactive-"):
            env = build_env()
            canonical = dashed_to_canonical(base_name[len("interactive-"):])
            sess_key = interactive_session_key(canonical)

            # Check if restart requested
            if arguments and arguments.get('restart', False):
                # Mark this specific session for restart if it exists
                if sess_key in sessions:
                    sessions[sess_key]['needs_restart'] = True

            # Resolve task to inspect metadata
            _, _, task_obj = resolve_task_and_args(canonical_to_dashed(canonical), arguments)

            # Ensure the async requirement is started first if defined (value is canonical like 'db.client')
            async_requires = getattr(task_obj.func, 'mcp_async_requires', None)
            if async_requires:
                await ensure_interactive_started(async_requires, env)

            # Ensure this interactive session is running
            await ensure_interactive_started(canonical, env, arguments)

            # Now interact (write input/wait) and return session state
            state = await interact_with_session(sess_key, arguments or {})
            return format_state_as_text(state, base_name)

        # Resolve and run a blocking tool
        runner_task, task_args, _ = resolve_task_and_args(base_name, arguments)
        cmd = build_runner_cmd(runner_task, task_args)
        env = build_env()
        # Ensure any lingering interactive environment is down before blocking
        pre = await stop_interactive_environment()
        state = await run_sync_collect(base_name, cmd, env)
        if pre:
            state["output"] = pre + state.get("output", [])
        return format_state_as_text(state, base_name)

    except Exception as e:
        with suppress(Exception):
            if base_name in sessions:
                sessions[base_name]['proc'].terminate()
                ensure_session_cleanup(base_name)
        return format_state_as_text({
            "error": str(e),
            "stacktrace": traceback.format_exception(type(e), e, e.__traceback__)
        }, base_name)


@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """
    List available tools.
    Each tool specifies its arguments using JSON Schema validation.
    """
    try:
        tasks = environment.get_tasks()
        tools = []
        interactive_tools = []

        for task in tasks:
            # warning: dots and slashes are not allowed in tool names
            task_name = task.name.replace(".", "-")
            if not hasattr(task.func, 'mcp_enabled'):
                continue

            properties, required = map_task_options(task)

            tool = types.Tool(
                name=task_name,
                description=f"{task.description} (blocking call)",
                inputSchema={
                    "type": "object",
                    "properties": properties,
                    "required": required
                }
            )
            if hasattr(task.func, 'mcp_interactive'):
                interactive_tools.append(
                    types.Tool(
                        name=f"interactive-{task_name}",
                        description=f"{task.description}",
                        inputSchema=INTERACT_INPUT
                    )
                )
            else:
                tools.append(tool)

        restart_environment = types.Tool(
            name="restart_environment",
            description="(re)start the interactive environment to reset everything cleanly",
            inputSchema = RESTART_INPUT
        )
        return [restart_environment] + sorted(interactive_tools, key=lambda x: x.name) + sorted(tools, key=lambda x: x.name)

    except Exception as e:
        # Return a single tool capturing the exception message
        return [
            types.Tool(
                name="error",
                description="error generating tool listing: " + ''.join(traceback.format_exception(type(e), e, e.__traceback__)),
                inputSchema={"type": "object", "properties": {}}
            )
        ]


@server.list_resources()
async def handle_list_resources() -> list[types.Resource]:
    """List resources (not implemented)."""
    # we do not implement resources yet, as we don't store anything in the runner
    return []


async def run_server():
    """Run the MCP server using stdio."""
    # CRITICAL: Redirect all logging to stderr to avoid corrupting MCP JSON-RPC on stdout
    import logging
    logging.basicConfig(
        stream=sys.stderr,  # Use stderr instead of stdout
        level=logging.WARNING,  # Reduce noise, only warnings and errors
        format='[MCP-%(levelname)s] %(message)s'
    )
    # Suppress all existing loggers that might write to stdout
    for logger_name in logging.root.manager.loggerDict:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    # first stop any old running mcp container
    subprocess.run(['docker', 'rm', '-f', CONTAINER_NAME],
                   stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL)
    # always run mcp server with stacktraces
    sys.tracebacklimit = 100
    # Run the server using stdin/stdout streams
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="Raptor Runner",
                server_version="0.1.0",
                instructions=DESCRIPTION,
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={}
                ),
            ),
        )
