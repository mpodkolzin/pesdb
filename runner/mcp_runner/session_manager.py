"""
Session lifecycle management for MCP server.

Handles creation, interaction, and cleanup of interactive and synchronous sessions.
"""

import asyncio
import re
import traceback
import subprocess
from typing import Any, Dict
from contextlib import suppress
from typing import TypedDict

from mcp_runner.constants import *
from mcp_runner.task_resolver import *

class Session(TypedDict):
    proc: asyncio.subprocess.Process
    output_buf: list[str]
    t_out: asyncio.Task[Any]
    exception: str
    prompt_pattern: str | None  # Regex pattern to detect when command is complete
    needs_restart: bool

# Global sessions dictionary
sessions: dict[str, Session] = {}

async def _pump_stream(reader: asyncio.StreamReader, buffer: list[str]):
    """Continuously read lines from reader into buffer."""
    try:
        while True:
            line = await reader.readline()
            if not line:
                break
            if isinstance(line, bytes):
                buffer.append(line.decode(errors="replace"))
            else:
                buffer.append(str(line))
    except Exception as e:
        # Append exception marker into the buffer so it is visible to callers
        buffer.append(f"\n[runner] stream exception: {e}\n")


def append_exception(sess: dict, message: str, e: Exception = None):
    """Append exception information to session exception string."""
    sep = "\n" if sess.get('exception') else ""
    sess['exception'] += sep + message
    if e:
        sess['exception'] += "\n".join(traceback.format_exception(type(e), e, e.__traceback__))


async def terminate_gracefully(proc: asyncio.subprocess.Process):
    """Send SIGTERM in a loop (every 0.3s) up to KILL_GRACE_SECONDS; then SIGKILL if still alive."""
    # Repeated terminate-wait-short pattern to handle stubborn subprocesses
    deadline = asyncio.get_event_loop().time() + KILL_GRACE_SECONDS
    while proc.returncode is None and asyncio.get_event_loop().time() < deadline:
        with suppress(Exception):
            proc.terminate()
        try:
            # Short wait between repeated terminate attempts
            await asyncio.wait_for(proc.wait(), timeout=0.3)
            break
        except asyncio.TimeoutError:
            # Not exited yet; loop and send terminate again until deadline
            continue
    if proc.returncode is None:
        with suppress(Exception):
            proc.kill()


def build_session_state(session_key: str) -> dict:
    """Create a standardized state dict from a session keyed by task name."""
    sess = sessions.get(session_key)
    if not sess:
        return {"error": f"No session for '{session_key}'."}
    proc = sess['proc']
    status = "running" if proc.returncode is None else f"stopped (exit code={proc.returncode})"
    state = {
        "status": status,
        "output": sess['output_buf'][:]
    }
    # Clear in place, otherwise pump_stream breaks
    sess['output_buf'].clear()
    if sess.get('exception'):
        state["exception"] = sess['exception']
        # Clear after reporting so it doesn't persist
        sess['exception'] = ""
    return state


def session_is_running(session_key: str) -> bool:
    """Check if a session exists and its process is running."""
    sess = sessions.get(session_key)
    return bool(sess and sess['proc'].returncode is None)


async def ensure_interactive_started(canonical_task: str, env: dict, arguments: dict | None = None) -> None:
    """Ensure an interactive tool for the canonical task name (e.g., 'db.client') is running.
    Starts it and waits for prompt_pattern if defined.
    """
    sess_key = interactive_session_key(canonical_task)
    sess = sessions.get(sess_key)

    old_output = None

    # If marked for restart, kill it first and preserve pending output
    if sess and sess.get('needs_restart', False):
        # Preserve any pending output from the old session
        old_output = sess['output_buf'][:]
        await terminate_gracefully(sess['proc'])
        with suppress(Exception):
            sess['t_out'].cancel()
            sess['proc'].stdin.close()
        sess['needs_restart'] = False
        # Prepend restart warning and old output to the buffer that will be reused
        sess['output_buf'][:] = [INTERACTIVE_RESTARTED] + old_output
        # Fall through to restart below
    elif session_is_running(sess_key):
        return

    runner_task, task_args, task_obj = resolve_task_and_args(canonical_to_dashed(canonical_task), arguments)
    cmd = build_runner_cmd(runner_task, task_args)
    prompt_pattern = getattr(task_obj.func, 'mcp_prompt_pattern', None)
    await start_async_session(sess_key, cmd, env, prompt_pattern=prompt_pattern)


async def start_async_session(session_key: str, cmd: list[str], env: dict, *, prompt_pattern: str | None = None) -> Dict[str, Any]:
    """Start (or refuse to start) a session under session_key. Returns state or error dict."""
    existing = sessions.get(session_key)
    if existing and existing['proc'].returncode is None:
        return {"error": f"Session '{session_key}' already running; stop it before starting a new one"}

    # Create empty session if it doesn't exist
    if not existing:
        sessions[session_key] = Session(
            proc=None,  # type: ignore[arg-type]
            output_buf=[],
            t_out=None,  # type: ignore[arg-type]
            exception="",
            prompt_pattern=None,
            needs_restart=False,
        )
        existing = sessions[session_key]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,  # merge stderr into stdout for ordering
        env=env,
    )

    # Update session with new process
    t_out = asyncio.create_task(_pump_stream(proc.stdout, existing['output_buf']))  # type: ignore[arg-type]
    existing['proc'] = proc
    existing['t_out'] = t_out
    existing['exception'] = ""
    existing['prompt_pattern'] = prompt_pattern
    existing['needs_restart'] = False
    # Optionally wait for a readiness pattern (the prompt) in output
    if prompt_pattern:
        try:
            pattern = re.compile(prompt_pattern)
        except re.error:
            pattern = None
        if pattern:
            waited = 0.0
            while existing['proc'].returncode is None and waited < WAIT_TIMEOUT:
                try:
                    if any(pattern.search(line) for line in existing['output_buf']):
                        break
                except Exception:
                    break
                await asyncio.sleep(0.05)
                waited += 0.05
    else:
        # Allow initial output to accumulate briefly
        with suppress(Exception):
            await asyncio.sleep(1.0)


async def interact_with_session(session_key: str, arguments: dict) -> Dict[str, Any]:
    """Send input (optional) and return structured state for a session."""
    sess = sessions.get(session_key)
    if not sess:
        return {"error": f"No session for '{session_key}'."}
    proc = sess['proc']

    # Send input if provided (only if process is still running)
    if 'input' in arguments and arguments['input'] is not None and proc.returncode is None:
        inp = arguments['input']
        if not isinstance(inp, list):
            return {"error": "input must be an array of strings"}
        # Only append empty line if we're actually sending input (non-empty list)
        # This prevents matching old prompt before new output arrives
        if inp:
            sess['output_buf'].append("")
        # Write each element as a separate line
        for item in inp:
            s = item if isinstance(item, str) else str(item)
            if not s.endswith('\n'):
                s += '\n'
            try:
                proc.stdin.write(s.encode())
                await proc.stdin.drain()  # type: ignore[attr-defined]
            except Exception as e:
                append_exception(sess, "stdin drain error:", e)
                break  # Stop trying to write if we get an error

    # Wait for prompt or timeout
    timeout = min(arguments.get('timeout', OUTPUT_FLUSH_WAIT_SECONDS), MAX_WAIT_TIMEOUT)
    reached_prompt = False

    if sess['prompt_pattern'] and proc.returncode is None:
        # Wait for prompt pattern with timeout
        try:
            pattern = re.compile(sess['prompt_pattern'])
            deadline = asyncio.get_event_loop().time() + timeout

            while asyncio.get_event_loop().time() < deadline and proc.returncode is None:
                # Check if the last line of output matches the prompt pattern
                if sess['output_buf']:
                    last_line = sess['output_buf'][-1].strip()
                    if pattern.fullmatch(last_line):
                        reached_prompt = True
                        break
                await asyncio.sleep(0.05)
        except re.error:
            pass  # Invalid regex, fall back to timeout
    else:
        # No prompt pattern, just wait the specified time
        with suppress(Exception):
            await asyncio.sleep(float(timeout))

    # Build state first, then opportunistically cleanup if process exited
    state = build_session_state(session_key)
    state['reached_prompt'] = reached_prompt

    if proc.returncode is not None:
        ensure_session_cleanup(session_key)
    return state


async def stop_session(session_key: str) -> Dict[str, Any]:
    """Stop a running session and return its final state."""
    sess = sessions.get(session_key)
    if not sess:
        return {"error": f"No session for '{session_key}'."}
    proc = sess['proc']
    await terminate_gracefully(proc)
    # Build state first, then cleanup to avoid losing buffers / session before reporting
    state = build_session_state(session_key)
    ensure_session_cleanup(session_key)
    return state


def ensure_session_cleanup(session_id: str):
    """If a session's process has exited, clean up tasks and stdin and remove only that session_id."""
    sess = sessions.get(session_id)
    if not sess:
        return
    proc = sess['proc']
    if proc.returncode is None:
        return
    with suppress(Exception):
        sess['t_out'].cancel()
        proc.stdin.close()
    sessions.pop(session_id, None)


def cleanup_all_sessions():
    """Best-effort: kill/cleanup all running sessions. Safe to call multiple times."""
    with suppress(Exception):
        for key in list(sessions.keys()):
            sessions[key]['proc'].terminate()


async def stop_interactive_environment(restart: bool = False) -> list[str]:
    """Stop all running interactive sessions and optionally kill container."""
    # Stop async sessions
    output = []
    if sessions:
        output = [INTERACTIVE_RESTARTED if restart else INTERACTIVE_STOPPED]
        for key in list(sessions.keys()):
            state = await stop_session(key)
            output.extend(state.get('output', []))
    # always stop the container for safety, so that nothing is left behind.
    subprocess.run(['docker', 'rm', '-f', CONTAINER_NAME],
                   stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL)
    return output


async def run_sync_collect(task_name: str, cmd: list[str], env: dict) -> Dict[str, Any]:
    """Run process synchronously using the async session machinery and return standardized JSON content."""
    session_key = f"{task_name}-sync"
    await start_async_session(session_key, cmd, env)
    sess = sessions[session_key]
    proc = sess['proc']
    try:
        # Wait until process finishes while pumps fill buffers, with timeout
        try:
            await asyncio.wait_for(proc.wait(), timeout=MAX_WAIT_TIMEOUT)
        except asyncio.TimeoutError:
            pass
        # Join pump tasks to ensure buffers flushed
        # Use wait_for to actually wait for the pump task to complete
        try:
            await asyncio.wait_for(sess['t_out'], timeout=OUTPUT_FLUSH_WAIT_SECONDS)
        except asyncio.TimeoutError:
            # If pump task doesn't finish in time, that's okay - we'll get what we have
            pass
        return build_session_state(session_key)
    finally:
        # Ensure process is down and cleanup
        await terminate_gracefully(proc)
        ensure_session_cleanup(session_key)
