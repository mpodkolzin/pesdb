"""Tests for mcp_runner decorators."""

from mcp_runner.decorators import mcp_enabled


def test_decorator_no_args():
    """Test @mcp_enabled without arguments."""
    @mcp_enabled
    def task():
        pass

    assert hasattr(task, 'mcp_enabled')
    assert task.mcp_enabled is True


def test_decorator_empty_call():
    """Test @mcp_enabled() with empty call."""
    @mcp_enabled()
    def task():
        pass

    assert hasattr(task, 'mcp_enabled')
    assert task.mcp_enabled is True


def test_decorator_interactive():
    """Test decorator with interactive=True."""
    @mcp_enabled(interactive=True)
    def task():
        pass

    assert task.mcp_enabled is True
    assert hasattr(task, 'mcp_interactive')
    assert task.mcp_interactive is True


def test_decorator_async_requires():
    """Test decorator with async_requires parameter."""
    @mcp_enabled(async_requires='db.client')
    def task():
        pass

    assert task.mcp_enabled is True
    assert hasattr(task, 'mcp_async_requires')
    assert task.mcp_async_requires == 'db.client'


def test_decorator_prompt_pattern():
    """Test decorator with prompt_pattern."""
    @mcp_enabled(prompt_pattern=r'\$ $')
    def task():
        pass

    assert task.mcp_enabled is True
    assert hasattr(task, 'mcp_prompt_pattern')
    assert task.mcp_prompt_pattern == r'\$ $'

def test_decorator_all_parameters():
    """Test decorator with all parameters combined."""
    @mcp_enabled(
        interactive=True,
        async_requires='other.task',
        prompt_pattern=r'>>> '
    )
    def task():
        return "result"

    assert task.mcp_enabled is True
    assert task.mcp_interactive is True
    assert task.mcp_async_requires == 'other.task'
    assert task.mcp_prompt_pattern == r'>>> '
    # Verify function still works
    assert task() == "result"


def test_decorator_preserves_function():
    """Test that decorator doesn't modify function behavior."""
    @mcp_enabled()
    def add(a, b):
        """Add two numbers."""
        return a + b

    assert add(2, 3) == 5
    assert add.__doc__ == "Add two numbers."
    assert add.__name__ == "add"
