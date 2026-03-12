"""Pytest configuration and shared fixtures for mcp_runner tests."""

import sys
from pathlib import Path

# Add parent directory to path so tests can import mcp_runner modules
sys.path.insert(0, str(Path(__file__).parent.parent))

# Configure pytest-asyncio
import pytest


@pytest.fixture(scope="session")
def event_loop_policy():
    """Set event loop policy for async tests."""
    import asyncio
    return asyncio.DefaultEventLoopPolicy()


# Suppress pytest-asyncio warnings about default fixture loop scope
def pytest_configure(config):
    """Configure pytest with asyncio settings."""
    config.option.asyncio_default_fixture_loop_scope = "function"


@pytest.fixture
def use_test_pavement(monkeypatch):
    """Patch build_runner_cmd to use test pavement instead of docker.

    This allows end-to-end testing of blocking tasks without requiring a docker environment.
    The tasks are executed via paver directly using the test pavement file.

    Usage:
        @pytest.mark.asyncio
        async def test_something(test_tasks, use_test_pavement):
            result = await handlers.handle_call_tool("blocking_test", {})
            # Now the task actually runs via paver, not docker
    """
    import sys
    from mcp_runner import task_resolver, mcp_handlers, session_manager

    test_pavement_path = Path(__file__).parent / "pavement.py"

    def mock_build_runner_cmd(runner_task: str, task_args: list[str]) -> list[str]:
        """Build command to run paver with test pavement instead of docker."""
        # Use sys.executable to ensure we run with the same Python/venv as the tests
        return [sys.executable, '-m', 'paver', '-f', str(test_pavement_path), runner_task, *task_args]

    # Patch in all places where build_runner_cmd is imported via "from X import *"
    monkeypatch.setattr(task_resolver, 'build_runner_cmd', mock_build_runner_cmd)
    monkeypatch.setattr(mcp_handlers, 'build_runner_cmd', mock_build_runner_cmd)
    monkeypatch.setattr(session_manager, 'build_runner_cmd', mock_build_runner_cmd)
