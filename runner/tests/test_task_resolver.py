"""Tests for task resolver module."""

import pytest
from paver.tasks import Task
from mcp_runner import task_resolver as tr
from mcp_runner.decorators import mcp_enabled


@pytest.fixture
def mock_environment(monkeypatch):
    """Mock paver environment with test tasks."""
    tasks = set()

    @mcp_enabled()
    def test_task():
        """Test task"""
        pass
    test_task_obj = Task(test_task)
    test_task_obj.name = "test.task"
    test_task_obj.user_options = []
    tasks.add(test_task_obj)

    @mcp_enabled()
    def blocking_task():
        """Blocking task"""
        pass
    blocking_task_obj = Task(blocking_task)
    blocking_task_obj.name = "blocking.task"
    blocking_task_obj.user_options = [
        ('details=', 'd', 'Show details', 'default_val'),
        ('verbose', 'v', 'Verbose output'),
    ]
    tasks.add(blocking_task_obj)

    def disabled_task():
        """Task without mcp_enabled"""
        pass
    disabled_task_obj = Task(disabled_task)
    disabled_task_obj.name = "disabled.task"
    disabled_task_obj.user_options = []
    tasks.add(disabled_task_obj)

    from paver.tasks import environment
    monkeypatch.setattr(environment, 'get_tasks', lambda: tasks)

    return tasks


def test_resolve_valid_task(mock_environment):
    """Test resolving valid task with mcp_enabled."""
    runner_task, args, task_obj = tr.resolve_task_and_args("test-task", None)

    assert runner_task == "test.task"
    assert args == []
    assert task_obj.name == "test.task"


def test_resolve_task_with_args(mock_environment):
    """Test resolving task with arguments."""
    runner_task, args, task_obj = tr.resolve_task_and_args(
        "blocking-task",
        {"details": "full", "verbose": True}
    )

    assert runner_task == "blocking.task"
    assert "--details=full" in args
    assert "--verbose" in args


def test_resolve_task_filters_unknown_args(mock_environment):
    """Test that unknown arguments are filtered out."""
    runner_task, args, task_obj = tr.resolve_task_and_args(
        "blocking-task",
        {"details": "full", "unknown": "value", "_meta": "ignored"}
    )

    assert "--details=full" in args
    assert "--unknown=value" not in args
    assert "--_meta=ignored" not in args


def test_resolve_task_disabled_raises_error(mock_environment):
    """Test that task without mcp_enabled raises error."""
    with pytest.raises(ValueError, match="Unknown tool: disabled-task"):
        tr.resolve_task_and_args("disabled-task", None)


def test_resolve_unknown_task_raises_error(mock_environment):
    """Test that unknown task name raises error."""
    with pytest.raises(ValueError, match="Unknown tool: nonexistent"):
        tr.resolve_task_and_args("nonexistent", None)


def test_build_runner_cmd():
    """Test building runner command."""
    cmd = tr.build_runner_cmd("test.task", ["--arg1", "--arg2=value"])

    assert cmd[0] == "runner/runner"
    assert "docker.attach_to=" in cmd[1]
    assert cmd[2] == "test.task"
    assert "--arg1" in cmd
    assert "--arg2=value" in cmd


def test_build_env():
    """Test building environment."""
    import os
    original_val = os.environ.get('TEST_VAR')
    os.environ['TEST_VAR'] = 'preserved'

    env = tr.build_env()

    assert env['MCP_RUNNER'] == '1'
    assert env['TEST_VAR'] == 'preserved'

    if original_val:
        os.environ['TEST_VAR'] = original_val
    else:
        os.environ.pop('TEST_VAR', None)


def test_map_task_options_tuple_style():
    """Test mapping tuple-style task options."""
    from paver.tasks import Task

    def task_func():
        pass

    task = Task(task_func)
    task.user_options = [
        ('output=', 'o', 'Output file'),
        ('verbose', 'v', 'Verbose mode'),
    ]

    properties, required = tr.map_task_options(task)

    assert 'output' in properties
    assert properties['output']['type'] == 'string'
    assert properties['output']['description'] == 'Output file'

    assert 'verbose' in properties
    assert properties['verbose']['type'] == 'boolean'


def test_map_task_options_with_llm_require():
    """Test that LLM_REQUIRE marks parameter as required."""
    from paver.tasks import Task

    def task_func():
        pass

    task = Task(task_func)
    task.user_options = [
        ('details=', 'd', 'Show details LLM_REQUIRE'),
    ]

    properties, required = tr.map_task_options(task)

    assert 'details' in required


def test_convert_stdin_input_list():
    """Test converting list to stdin bytes."""
    result = tr.convert_stdin_input(['line1', 'line2', 'line3'])

    assert result == b'line1\nline2\nline3\n'


def test_convert_stdin_input_preserves_newlines():
    """Test that existing newlines are preserved."""
    result = tr.convert_stdin_input(['line1\n', 'line2'])

    assert result == b'line1\nline2\n'


def test_convert_stdin_input_empty():
    """Test converting empty input."""
    result = tr.convert_stdin_input([])
    assert result == b''

    result = tr.convert_stdin_input(None)
    assert result == b''


def test_canonical_to_dashed():
    """Test converting canonical names to dashed."""
    assert tr.canonical_to_dashed("db.client") == "db-client"
    assert tr.canonical_to_dashed("test.deep.nested") == "test-deep-nested"
    assert tr.canonical_to_dashed("no-dots") == "no-dots"


def test_dashed_to_canonical():
    """Test converting dashed names to canonical."""
    assert tr.dashed_to_canonical("db-client") == "db.client"
    assert tr.dashed_to_canonical("test-deep-nested") == "test.deep.nested"
    assert tr.dashed_to_canonical("no_dashes") == "no_dashes"


def test_interactive_session_key():
    """Test generating interactive session keys."""
    key = tr.interactive_session_key("db.client")
    assert key == "interactive-db-client"

    key = tr.interactive_session_key("shell.root")
    assert key == "interactive-shell-root"
