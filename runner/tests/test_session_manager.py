"""Tests for session manager module."""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from mcp_runner import session_manager as sm


@pytest.fixture
def clean_sessions():
    """Clear sessions before and after each test."""
    sm.sessions.clear()
    yield
    sm.sessions.clear()


def test_session_is_running(clean_sessions):
    """Test checking if session is running."""
    # No session
    assert not sm.session_is_running("test-session")

    # Session with running process
    proc = MagicMock()
    proc.returncode = None
    sm.sessions["test-session"] = {
        'proc': proc,
        'output_buf': [],
        't_out': None,
        'exception': "",
        'prompt_pattern': None,
        'needs_restart': False,
    }
    assert sm.session_is_running("test-session")

    # Session with stopped process
    proc.returncode = 0
    assert not sm.session_is_running("test-session")


def test_build_session_state(clean_sessions):
    """Test building session state dict."""
    proc = MagicMock()
    proc.returncode = None
    sm.sessions["test-session"] = {
        'proc': proc,
        'output_buf': ['line1', 'line2'],
        't_out': None,
        'exception': "",
        'prompt_pattern': r'\$ $',
        'needs_restart': False,
    }

    state = sm.build_session_state("test-session")

    assert state['status'] == 'running'
    assert state['output'] == ['line1', 'line2']
    assert 'returncode' not in state


def test_build_session_state_stopped(clean_sessions):
    """Test building state for stopped session."""
    proc = MagicMock()
    proc.returncode = 0
    sm.sessions["test-session"] = {
        'proc': proc,
        'output_buf': ['done'],
        't_out': None,
        'exception': "",
        'prompt_pattern': None,
        'needs_restart': False,
    }

    state = sm.build_session_state("test-session")

    assert state['status'] == 'stopped (exit code=0)'
    assert 'done' in state['output']


def test_build_session_state_with_exception(clean_sessions):
    """Test building state when exception occurred."""
    proc = MagicMock()
    proc.returncode = None
    sm.sessions["test-session"] = {
        'proc': proc,
        'output_buf': [],
        't_out': None,
        'exception': "Test error",
        'prompt_pattern': None,
        'needs_restart': False,
    }

    state = sm.build_session_state("test-session")

    assert 'exception' in state
    assert 'Test error' in state['exception']


def test_append_exception(clean_sessions):
    """Test appending exception to session."""
    sm.sessions["test-session"] = {
        'proc': None,
        'output_buf': [],
        't_out': None,
        'exception': "",
        'prompt_pattern': None,
        'needs_restart': False,
    }

    sm.append_exception(sm.sessions["test-session"], "Error:", ValueError("test"))

    assert "Error:" in sm.sessions["test-session"]['exception']
    assert "ValueError" in sm.sessions["test-session"]['exception']


@pytest.mark.asyncio
async def test_terminate_gracefully():
    """Test graceful process termination."""
    proc = AsyncMock()
    proc.returncode = None
    proc.terminate = MagicMock()
    proc.kill = MagicMock()

    # Process terminates after SIGTERM
    async def wait_mock():
        proc.returncode = 0

    proc.wait = wait_mock

    await sm.terminate_gracefully(proc)

    proc.terminate.assert_called_once()
    proc.kill.assert_not_called()


@pytest.mark.asyncio
async def test_terminate_gracefully_requires_sigkill():
    """Test termination when SIGKILL is needed."""
    proc = AsyncMock()
    proc.returncode = None
    proc.terminate = MagicMock()
    proc.kill = MagicMock()

    # Process doesn't terminate after SIGTERM
    wait_count = [0]

    async def wait_mock():
        wait_count[0] += 1
        if wait_count[0] > 1:
            proc.returncode = -9

    proc.wait = wait_mock

    # Mock sleep to speed up test
    with patch('asyncio.sleep', new_callable=AsyncMock):
        await sm.terminate_gracefully(proc)

    proc.terminate.assert_called()
    proc.kill.assert_called_once()


@pytest.mark.asyncio
async def test_stop_interactive_environment(clean_sessions):
    """Test stopping all interactive sessions."""
    # Create mock sessions
    proc1 = MagicMock()
    proc1.returncode = None
    proc1.stdin = MagicMock()
    proc1.stdin.close = MagicMock()
    proc2 = MagicMock()
    proc2.returncode = None
    proc2.stdin = MagicMock()
    proc2.stdin.close = MagicMock()

    # Use MagicMock for tasks (cancel is synchronous)
    task1 = MagicMock()
    task1.cancel = MagicMock()
    task2 = MagicMock()
    task2.cancel = MagicMock()

    sm.sessions["session1"] = {
        'proc': proc1,
        'output_buf': ['output1'],
        't_out': task1,
        'exception': "",
        'prompt_pattern': None,
        'needs_restart': False,
    }
    sm.sessions["session2"] = {
        'proc': proc2,
        'output_buf': ['output2'],
        't_out': task2,
        'exception': "",
        'prompt_pattern': None,
        'needs_restart': False,
    }

    async def terminate_side_effect(proc):
        """Mock terminate_gracefully to set returncode."""
        proc.returncode = 0

    with patch('mcp_runner.session_manager.terminate_gracefully', new_callable=AsyncMock) as mock_terminate:
        mock_terminate.side_effect = terminate_side_effect
        with patch('subprocess.run') as mock_docker:
            output = await sm.stop_interactive_environment()

    assert len(output) > 0
    assert len(sm.sessions) == 0
    mock_docker.assert_called_once()


def test_ensure_session_cleanup(clean_sessions):
    """Test session cleanup for stopped process."""
    proc = MagicMock()
    proc.returncode = 0
    task = MagicMock()
    stdin = MagicMock()

    sm.sessions["test-session"] = {
        'proc': proc,
        'output_buf': [],
        't_out': task,
        'exception': "",
        'prompt_pattern': None,
        'needs_restart': False,
    }
    proc.stdin = stdin

    sm.ensure_session_cleanup("test-session")

    task.cancel.assert_called_once()
    stdin.close.assert_called_once()
    assert "test-session" not in sm.sessions


def test_ensure_session_cleanup_running_process(clean_sessions):
    """Test that cleanup doesn't remove running process."""
    proc = MagicMock()
    proc.returncode = None

    sm.sessions["test-session"] = {
        'proc': proc,
        'output_buf': [],
        't_out': None,
        'exception': "",
        'prompt_pattern': None,
        'needs_restart': False,
    }

    sm.ensure_session_cleanup("test-session")

    assert "test-session" in sm.sessions


@pytest.mark.asyncio
async def test_run_sync_collect_success():
    """Test running synchronous command successfully."""
    cmd = ['echo', 'test']
    env = {'TEST': 'value'}

    with patch('mcp_runner.session_manager.start_async_session', new_callable=AsyncMock) as mock_start:
        # Mock start_async_session to populate sessions dict
        async def start_side_effect(session_key, cmd, env, **kwargs):
            # Use MagicMock for proc with async wait method
            proc = MagicMock()
            proc.returncode = None
            proc.stdin = MagicMock()
            proc.stdin.close = MagicMock()

            async def wait_mock():
                proc.returncode = 0
            proc.wait = wait_mock

            # Create task mock that can be cancelled and awaited
            async def task_mock_coro():
                pass
            task_mock = asyncio.create_task(task_mock_coro())
            # Let it complete
            await task_mock
            # Create a new completed task for the session
            task_mock = asyncio.create_task(task_mock_coro())

            sm.sessions[session_key] = {
                'proc': proc,
                'output_buf': ['test\n'],
                't_out': task_mock,
                'exception': '',
                'prompt_pattern': None,
                'needs_restart': False,
            }
            return sm.build_session_state(session_key)

        mock_start.side_effect = start_side_effect

        with patch('mcp_runner.session_manager.terminate_gracefully', new_callable=AsyncMock):
            state = await sm.run_sync_collect("test", cmd, env)

    assert state['status'] == 'stopped (exit code=0)'
    assert 'output' in state


@pytest.mark.asyncio
async def test_run_sync_collect_failure():
    """Test running command that fails."""
    cmd = ['false']
    env = {}

    with patch('asyncio.create_subprocess_exec', new_callable=AsyncMock) as mock_create:
        proc = AsyncMock()
        proc.returncode = None
        proc.stdin = MagicMock()
        proc.stdin.close = MagicMock()
        proc.stdout = AsyncMock()
        proc.stdout.readline = AsyncMock(return_value=b'')

        async def wait_mock():
            proc.returncode = 1
        proc.wait = wait_mock

        mock_create.return_value = proc

        state = await sm.run_sync_collect("test", cmd, env)

    assert state['status'] == 'stopped (exit code=1)'
