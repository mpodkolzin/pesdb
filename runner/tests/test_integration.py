"""Integration tests for MCP server functionality."""

from pathlib import Path
import pytest
import pytest_asyncio

# Check if MCP SDK is available
pytest.importorskip("mcp.server.models", reason="MCP SDK not installed")

import mcp_runner.mcp_handlers as handlers
from mcp_runner.session_manager import sessions
from paver.tasks import environment


@pytest.fixture
def test_tasks():
    """Load test pavement and register test tasks."""
    import importlib.util
    import os

    test_pavement_path = Path(__file__).parent / "pavement.py"
    spec = importlib.util.spec_from_file_location("test_pavement", test_pavement_path)
    test_pavement = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(test_pavement)

    # Collect all tasks
    tasks_to_register = [
        test_pavement.echo_shell,
        test_pavement.stateful_shell,
        test_pavement.python_style_shell,
        test_pavement.dependency_task,
        test_pavement.task_with_needs,
        test_pavement.blocking_test,
        test_pavement.blocking_large_output,
        test_pavement.pty_echo_shell,
    ]

    # Set names without module prefix
    for task in tasks_to_register:
        task.name = task.func.__name__

    tasks_set = environment.get_tasks()
    for task in tasks_to_register:
        tasks_set.add(task)

    old_pavement = os.environ.get('PAVEMENT_FILE')
    os.environ['PAVEMENT_FILE'] = str(test_pavement_path)

    yield

    for task in tasks_to_register:
        tasks_set.discard(task)
    if old_pavement:
        os.environ['PAVEMENT_FILE'] = old_pavement
    else:
        os.environ.pop('PAVEMENT_FILE', None)


@pytest.fixture
def clean_sessions():
    """Clear sessions before each test."""
    sessions.clear()
    yield
    sessions.clear()


@pytest.mark.asyncio
async def test_tool_listing(test_tasks):
    """Test that tool listing includes expected tools."""
    tools = await handlers.handle_list_tools()
    tool_names = [t.name for t in tools]

    assert "interactive-echo_shell" in tool_names
    assert "blocking_test" in tool_names
    assert "restart_environment" in tool_names


@pytest.mark.asyncio
async def test_interactive_shell_state_persistence(test_tasks, clean_sessions):
    """Test that interactive shell maintains state across invocations."""
    tool_name = "interactive-echo_shell"

    # First invocation
    result1 = await handlers.handle_call_tool(
        tool_name,
        {"input": ["hello world"], "timeout": 5}
    )
    assert "hello world" in result1[0].text

    # Second invocation - session should persist
    result2 = await handlers.handle_call_tool(
        tool_name,
        {"input": ["second command"], "timeout": 5}
    )
    assert "second command" in result2[0].text

    # Third invocation
    result3 = await handlers.handle_call_tool(
        tool_name,
        {"input": ["third command"], "timeout": 5}
    )
    assert "third command" in result3[0].text

    # Verify session is still running
    assert len(sessions) == 1
    session_key = list(sessions.keys())[0]
    assert sessions[session_key]['proc'].returncode is None


@pytest.mark.asyncio
async def test_blocking_tool_kills_interactive_session(test_tasks, clean_sessions, use_test_pavement):
    """Test that blocking tool calls terminate interactive sessions."""
    # Start an interactive session
    await handlers.handle_call_tool(
        "interactive-echo_shell",
        {"input": ["test"], "timeout": 5}
    )
    assert len(sessions) == 1

    # Call a blocking tool
    result = await handlers.handle_call_tool("blocking_test", None)

    # Verify sessions were cleaned up
    if len(sessions) > 0:
        for key, sess in sessions.items():
            assert sess['proc'].returncode is not None

    # Check output mentions session was stopped
    assert "WARNING: The interactive session has been stopped" in result[0].text or len(sessions) == 0

    # With the fixture, we can also verify the task output
    assert "Blocking tool executed" in result[0].text, "Should contain the blocking tool output"


@pytest.mark.asyncio
async def test_restart_environment_cleans_sessions(test_tasks, clean_sessions):
    """Test that restart_environment cleans up all sessions."""
    # Start an interactive session
    await handlers.handle_call_tool(
        "interactive-echo_shell",
        {"input": ["test"], "timeout": 5}
    )
    assert len(sessions) >= 1

    # Call restart_environment
    result = await handlers.handle_call_tool("restart_environment", None)

    # Verify all sessions are cleaned up
    assert len(sessions) == 0
    assert "restarted" in result[0].text.lower()


@pytest.mark.asyncio
async def test_stateful_shell_maintains_state(test_tasks, clean_sessions):
    """Test that interactive shell truly maintains state across invocations."""
    tool_name = "interactive-stateful_shell"

    # First invocation - increment counter
    result1 = await handlers.handle_call_tool(
        tool_name,
        {"input": ["INCREMENT"], "timeout": 5}
    )
    assert "Counter: 1" in result1[0].text

    # Second invocation - increment again (should be 2, not 1)
    result2 = await handlers.handle_call_tool(
        tool_name,
        {"input": ["INCREMENT"], "timeout": 5}
    )
    assert "Counter: 2" in result2[0].text

    # Third invocation - get counter value
    result3 = await handlers.handle_call_tool(
        tool_name,
        {"input": ["GET"], "timeout": 5}
    )
    assert "Counter: 2" in result3[0].text

    # Fourth invocation - increment twice more
    result4 = await handlers.handle_call_tool(
        tool_name,
        {"input": ["INCREMENT", "INCREMENT"], "timeout": 5}
    )
    assert "Counter: 4" in result4[0].text

    # Fifth invocation - reset and verify
    result5 = await handlers.handle_call_tool(
        tool_name,
        {"input": ["RESET", "GET"], "timeout": 5}
    )
    assert "Counter reset" in result5[0].text
    assert "Counter: 0" in result5[0].text


@pytest.mark.asyncio
async def test_prompt_pattern_custom_ready(test_tasks, clean_sessions):
    """Test that READY> prompt pattern is correctly detected."""
    tool_name = "interactive-stateful_shell"

    result = await handlers.handle_call_tool(
        tool_name,
        {"input": ["GET"], "timeout": 5}
    )

    # Check that prompt was detected
    assert "READY>" in result[0].text or "Prompt: ✓ detected" in result[0].text


@pytest.mark.asyncio
async def test_prompt_pattern_python_style(test_tasks, clean_sessions):
    """Test that >>> prompt pattern is correctly detected."""
    tool_name = "interactive-python_style_shell"

    result = await handlers.handle_call_tool(
        tool_name,
        {"input": ["hello"], "timeout": 5}
    )

    # Check that output contains expected response
    assert "You said: hello" in result[0].text
    # Check that prompt was detected
    assert ">>>" in result[0].text or "Prompt: ✓ detected" in result[0].text


@pytest.mark.asyncio
async def test_prompt_pattern_custom(test_tasks, clean_sessions):
    """Test that custom PROMPT### pattern is correctly detected."""
    tool_name = "interactive-echo_shell"

    result = await handlers.handle_call_tool(
        tool_name,
        {"input": ["test"], "timeout": 5}
    )

    # Check that prompt was detected
    assert "PROMPT###" in result[0].text or "Prompt: ✓ detected" in result[0].text


@pytest.mark.asyncio
async def test_async_requires_dependency_execution_order(test_tasks, clean_sessions):
    """Test that async_requires parameter causes dependency to start before main task."""
    # Initially no sessions
    assert len(sessions) == 0

    # Call the main task which has async_requires='dependency_task'
    result = await handlers.handle_call_tool(
        "interactive-task_with_needs",
        {"input": ["test"], "timeout": 5}
    )

    # Verify both sessions were started (async_requires causes dependency to start)
    assert len(sessions) == 2, f"Expected 2 sessions, got {len(sessions)}: {list(sessions.keys())}"
    assert "interactive-dependency_task" in sessions, "Dependency task session not found"
    assert "interactive-task_with_needs" in sessions, "Main task session not found"

    # Verify main task responded
    output = result[0].text
    assert "Main got: test" in output or "MAIN_READY>" in output

    # Verify both are still running
    assert sessions["interactive-dependency_task"]['proc'].returncode is None
    assert sessions["interactive-task_with_needs"]['proc'].returncode is None


@pytest.mark.asyncio
async def test_async_requires_dependency_waits_for_prompt(test_tasks, clean_sessions):
    """Test that async_requires parameter waits for dependency to reach its prompt before starting main task."""
    # Call the main task which has async_requires='dependency_task'
    result = await handlers.handle_call_tool(
        "interactive-task_with_needs",
        {"input": ["test"], "timeout": 5}
    )

    output = result[0].text

    # The dependency should have started and reached its prompt
    # The dependency prints "DEPENDENCY_EXECUTED" on startup
    # Since ensure_interactive_started waits for the prompt, by the time task_with_needs starts,
    # the dependency should already have its prompt ready

    # Verify dependency session exists and is running
    assert "interactive-dependency_task" in sessions
    dep_sess = sessions["interactive-dependency_task"]
    assert dep_sess['proc'].returncode is None, "Dependency session should still be running"

    # Verify the dependency reached its prompt pattern (DEP_READY>)
    # The prompt pattern should be set
    assert dep_sess['prompt_pattern'] == r'DEP_READY>'

    # Since the main task successfully started and responded, the dependency must have reached its prompt
    # Otherwise start_async_session would still be waiting
    assert "Main got: test" in output or "MAIN_READY>" in output, "Main task should have responded"


@pytest.mark.asyncio
async def test_blocking_task_complete_output(test_tasks, clean_sessions, use_test_pavement):
    """Test that blocking tasks produce complete output even with large output.

    This test uses use_test_pavement fixture to run tasks via paver instead of docker,
    allowing true end-to-end subprocess testing of the output flushing behavior.

    The bug this test validates:
    - Before fix: timeout=0.1s in run_sync_collect was too short, causing output truncation
    - After fix: timeout=OUTPUT_FLUSH_WAIT_SECONDS (5.0s) allows full output to flush
    """
    # Call the blocking task that produces large output
    result = await handlers.handle_call_tool(
        "blocking_large_output",
        {}
    )

    output = result[0].text

    # Verify START_MARKER is in output
    assert "START_MARKER" in output, "Output should contain START_MARKER"

    # Verify END_MARKER is in output
    assert "END_MARKER" in output, "Output should contain END_MARKER at the end"

    # Verify we have a substantial amount of the output (at least some of the lines)
    # Count how many "Line X:" strings are present
    line_count = output.count("Line ")
    assert line_count >= 90, f"Should have at least 90 lines, got {line_count}. Output may have been truncated."

    # Verify the last lines (95-99) are present to ensure we got the end
    assert "Line 99:" in output, "Should have the last line (99) in output"


@pytest.mark.asyncio
async def test_initial_prompt_detection_with_empty_poll(test_tasks, clean_sessions):
    """Test that initial prompt is detected on first startup with empty poll.

    This validates the fix for the bug where empty line was appended even for
    empty input lists, preventing prompt detection on first startup.

    Bug: session_manager.py was appending empty line for input=[] which hid the prompt
    Fix: Only append empty line when actually sending non-empty input
    """
    tool_name = "interactive-echo_shell"

    # First call with empty input (just polling) - should detect initial prompt
    result = await handlers.handle_call_tool(
        tool_name,
        {"input": [], "timeout": 5}
    )

    output = result[0].text

    # Verify prompt was detected on first startup
    assert "PROMPT: ✓ detected - ready for next command" in output, \
        "Initial prompt should be detected on first startup with empty poll"

    # Verify the shell started (should see PROMPT### in output)
    assert "PROMPT###" in output, "Should see the shell's prompt in output"


@pytest.mark.asyncio
async def test_no_character_corruption_with_pty_echo(test_tasks, clean_sessions, use_test_pavement):
    """Test that PTY echo + application echo scenario doesn't cause corruption.

    This validates the fix for the actual PTY echo + psql -a double-echo bug that
    caused severe character corruption, reordering, and loss.

    Bug: script command creates PTY with echo enabled + psql -a also echoes input
         -> Race condition corrupts output with duplicated/reordered/lost characters
    Fix: Added 'stty -echo' to disable PTY echo, letting psql -a handle echoing cleanly

    This test uses pty_echo_shell which reproduces the actual scenario:
    - Uses 'script -q /dev/null -c ...' to create PTY (like db.client)
    - Inner script echoes input (like psql -a)
    - Includes 'stty -echo' to prevent double-echo (the fix)
    """
    tool_name = "interactive-pty_echo_shell"

    # Send a long, complex input that would trigger corruption bugs
    # Include characters that commonly get doubled/corrupted: commas, quotes, numbers
    long_text = "INSERT INTO orders (customer_name, amount, order_date) VALUES ('John Doe', 99.99, '2024-01-15'), ('Jane Smith', 149.50, '2024-01-16'), ('Bob Johnson', 75.25, '2024-01-17')"

    result = await handlers.handle_call_tool(
        tool_name,
        {"input": [long_text], "timeout": 5}
    )

    output = result[0].text

    # Verify the input was echoed back correctly without corruption
    assert long_text in output, \
        f"Long input should be echoed back without corruption.\n\nExpected:\n{long_text}\n\nGot output:\n{output}"

    # Verify no character duplication (common symptom of double-echo)
    # Check specific corruption patterns that occurred in the bug:
    # - Doubled commas: ",,"
    # - Missing leading characters
    # - Reordered characters

    # Count commas - should match the original (unless long_text already has ",,")
    expected_comma_count = long_text.count(",")
    # Allow for one extra comma if there's formatting
    actual_comma_count = output.count(",")
    assert actual_comma_count <= expected_comma_count + 2, \
        f"Too many commas in output (double-echo symptom). Expected ~{expected_comma_count}, got {actual_comma_count}"

    # Verify all key parts are present and in order
    assert "INSERT INTO orders" in output, "Should have complete INSERT statement"
    assert "John Doe" in output, "Should have first customer name"
    assert "99.99" in output, "Should have first amount"
    assert "Jane Smith" in output, "Should have second customer name"
    assert "149.50" in output, "Should have second amount"


@pytest.mark.asyncio
async def test_no_character_corruption_with_long_input(test_tasks, clean_sessions):
    """Test that long inputs work correctly with Python input() based shells.

    This is a simpler test using echo_shell (Python input()) to verify basic
    long input handling without the PTY complexity.
    """
    tool_name = "interactive-echo_shell"

    # Send a long, complex input
    long_text = "This is a very long command with many characters that could potentially get corrupted. It includes special characters like commas, apostrophes, numbers 123456789, and symbols !@#$%"

    result = await handlers.handle_call_tool(
        tool_name,
        {"input": [long_text], "timeout": 5}
    )

    output = result[0].text

    # Verify the input was echoed back correctly without corruption
    assert long_text in output, \
        f"Long input should be echoed back without corruption. Expected '{long_text}' in output"


@pytest.mark.asyncio
async def test_multiple_commands_no_corruption(test_tasks, clean_sessions):
    """Test that multiple commands in one call execute cleanly without corruption.

    This test validates that all commands in an input array are processed correctly
    and outputs don't get mangled or lost.
    """
    tool_name = "interactive-echo_shell"

    # Send multiple commands in one call
    commands = [
        "First command with some text",
        "Second command with numbers 123456",
        "Third command with special chars !@#$%"
    ]

    result = await handlers.handle_call_tool(
        tool_name,
        {"input": commands, "timeout": 5}
    )

    output = result[0].text

    # Verify all commands are in output
    for cmd in commands:
        assert cmd in output, f"Command '{cmd}' should appear in output without corruption"

    # Verify prompt was detected after all commands
    assert "PROMPT: ✓ detected" in output or "PROMPT###" in output, \
        "Prompt should be detected after multiple commands"


@pytest.mark.asyncio
async def test_empty_poll_after_commands(test_tasks, clean_sessions):
    """Test that empty poll after sending commands behaves correctly.

    After sending commands, polling with empty input should not re-report old output.
    The output buffer is cleared after each interaction by design.
    """
    tool_name = "interactive-echo_shell"

    # Send a command
    result1 = await handlers.handle_call_tool(
        tool_name,
        {"input": ["test command"], "timeout": 5}
    )
    assert "test command" in result1[0].text

    # Now poll with empty input - should have no output (buffer was cleared)
    result2 = await handlers.handle_call_tool(
        tool_name,
        {"input": [], "timeout": 1}  # Short timeout since we expect no new output
    )

    output = result2[0].text

    # Should not re-report the old "test command" output
    # The buffer is cleared after each interaction by build_session_state()
    assert "test command" not in output or output.count("test command") == 0, \
        "Empty poll should not re-report old output"


@pytest.mark.asyncio
async def test_prompt_detection_timing(test_tasks, clean_sessions):
    """Test that prompt detection works reliably across different timing scenarios.

    This validates that the prompt pattern matching in interact_with_session
    correctly uses fullmatch on the last line and doesn't get confused by
    empty lines or partial matches.
    """
    tool_name = "interactive-echo_shell"

    # Start the session
    result1 = await handlers.handle_call_tool(
        tool_name,
        {"input": [], "timeout": 5}
    )
    assert "PROMPT: ✓ detected" in result1[0].text, "Should detect initial prompt"

    # Send command and verify prompt detected after
    result2 = await handlers.handle_call_tool(
        tool_name,
        {"input": ["test"], "timeout": 5}
    )
    assert "PROMPT: ✓ detected" in result2[0].text, "Should detect prompt after command"

    # Send multiple commands and verify prompt detected
    result3 = await handlers.handle_call_tool(
        tool_name,
        {"input": ["cmd1", "cmd2", "cmd3"], "timeout": 5}
    )
    assert "PROMPT: ✓ detected" in result3[0].text, "Should detect prompt after multiple commands"
