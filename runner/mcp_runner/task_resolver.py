"""
Task resolution and command building utilities.

Handles mapping between MCP tool names and runner tasks,
argument conversion, and command construction.
"""

import os
from typing import Any
from paver.tasks import environment
from mcp_runner.constants import *


def resolve_task_and_args(name: str, arguments: dict | None) -> tuple[str, list[str], Any]:
    """
    Resolve MCP tool name to runner task and prepare arguments.

    Returns:
        (runner_task_name, task_args, task_object)
    """
    # make sure the task we get asked to execute is allowed
    tasks = environment.get_tasks()

    found_task = None
    runner_task = None
    for task in tasks:
        # warning: dots and slashes are not allowed in tool names
        task_name = task.name.replace(".", "-")
        runner_task = task.name
        if task_name == name and hasattr(task.func, 'mcp_enabled'):
            found_task = task
            break

    # Check if the requested task exists
    if not found_task:
        raise ValueError(f"Unknown tool: {name}")

    # Prepare arguments for the task, filtering only supported options
    task_args = []
    allowed_properties, _ = map_task_options(found_task)
    allowed_keys = set(allowed_properties.keys())
    if arguments:
        for arg_name, arg_value in arguments.items():
            if arg_name not in allowed_keys:
                # Ignore unknown / meta arguments (e.g., toolSummary, _meta, etc.)
                continue
            # Convert boolean values to flags (without value)
            if isinstance(arg_value, bool):
                if arg_value:
                    task_args.append(f"--{arg_name}")
            else:
                task_args.append(f"--{arg_name}={arg_value}")

    return runner_task, task_args, found_task


def build_runner_cmd(runner_task: str, task_args: list[str]) -> list[str]:
    """Construct the command to invoke the runner executable with arguments."""
    # Prefer explicit runner path to avoid EACCES issues
    return ['runner/runner', f'docker.attach_to={CONTAINER_NAME}', runner_task, *task_args]


def build_env() -> dict:
    """Create environment for subprocess execution."""
    env = os.environ.copy()
    env['MCP_RUNNER'] = '1'
    return env


def map_task_options(task) -> tuple[dict, list]:
    """Map paver task options to a JSON schema (properties, required)."""
    properties: dict = {}
    required: list = []
    for option in task.user_options:
        # Support both tuple/list style and optparse.Option from make_option
        default = None
        if isinstance(option, (tuple, list)) and len(option) >= 3:
            name = str(option[0]).replace('.', '-')
            if name.endswith('='):
                name = name[:-1]
                opt_type = "string"
            else:
                opt_type = "boolean"
            description = str(option[2])
        else:
            # optparse.Option path (duck-typed)
            dest = getattr(option, 'dest', None) or 'option'
            name = str(dest).replace('.', '-')
            action = getattr(option, 'action', None)
            opt_type = "boolean" if action in ("store_true", "store_false") else "string"
            description = getattr(option, 'help', "") or ""
            default = getattr(option, 'default', None)
        properties[name] = {
            "type": opt_type,
            "description": description
        }
        # Add default for optparse.Option if present
        if default is not None:
            properties[name]["default"] = default
        # Encourage including details to avoid double runs
        if name == "details" or description.endswith("LLM_REQUIRE"):
            required.append(name)
    return properties, required


def convert_stdin_input(data) -> bytes:
    """Normalize input (list[str]) to bytes, ensuring one newline per element."""
    if not data:
        return b""
    parts: list[str] = []
    for x in data:
        s = x if isinstance(x, str) else str(x)
        if not s.endswith('\n'):
            s += '\n'
        parts.append(s)
    return ''.join(parts).encode()


def canonical_to_dashed(name: str) -> str:
    """Convert canonical task name (with dots) to dashed format."""
    return name.replace('.', '-')


def dashed_to_canonical(name: str) -> str:
    """Convert dashed format back to canonical task name (with dots)."""
    return name.replace('-', '.')


def interactive_session_key(canonical_name: str) -> str:
    """Generate session key for interactive tool from canonical task name."""
    return f"interactive-{canonical_to_dashed(canonical_name)}"
