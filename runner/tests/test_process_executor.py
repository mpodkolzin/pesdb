"""Tests for process executor module."""

import mcp_runner.process_executor as pe


def test_format_state_running_process():
    """Test formatting state for running process."""
    state = {
        "status": "running",
        "output": ["Line 1", "Line 2", "Line 3"],
    }

    result = pe.format_state_as_text(state, "test-tool")

    assert len(result) == 1
    text = result[0].text
    assert "TOOL: test-tool" in text
    assert "STATUS: running" in text
    assert "Line 1" in text
    assert "Line 2" in text
    assert "Line 3" in text


def test_format_state_stopped_process():
    """Test formatting state for stopped process."""
    state = {
        "status": "stopped (exit code=0)",
        "output": ["Command completed"],
    }

    result = pe.format_state_as_text(state, "test-tool")

    assert len(result) == 1
    text = result[0].text
    assert "STATUS: stopped (exit code=0)" in text
    assert "Command completed" in text


def test_format_state_with_error():
    """Test formatting state with error."""
    state = {
        "error": "Something went wrong",
    }

    result = pe.format_state_as_text(state, "test-tool")

    assert len(result) == 1
    text = result[0].text
    assert "STATUS: unknown" in text
    assert "ERROR: Something went wrong" in text


def test_format_state_with_stacktrace():
    """Test formatting state with stacktrace."""
    state = {
        "error": "Failed",
        "stacktrace": "Traceback (most recent call last):\n  File ...",
    }

    result = pe.format_state_as_text(state, "test-tool")

    assert len(result) == 1
    text = result[0].text
    assert "ERROR: Failed" in text
    assert "STACKTRACE:" in text
    assert "Traceback" in text


def test_format_state_empty_output():
    """Test formatting state with empty output."""
    state = {
        "status": "running",
        "output": [],
    }

    result = pe.format_state_as_text(state, "test-tool")

    assert len(result) == 1
    text = result[0].text
    assert "STATUS: running" in text
    # Empty output doesn't include "Output:" section
    assert "Output:" not in text


def test_format_state_with_prompt_reached():
    """Test formatting state with reached_prompt indicator."""
    state = {
        "status": "running",
        "output": ["$ "],
        "reached_prompt": True,
    }

    result = pe.format_state_as_text(state, "interactive-shell")

    assert len(result) == 1
    text = result[0].text
    assert "PROMPT: ✓ detected" in text


def test_format_state_prompt_not_reached():
    """Test formatting state when prompt not reached."""
    state = {
        "status": "running",
        "output": ["Still running..."],
        "reached_prompt": False,
    }

    result = pe.format_state_as_text(state, "interactive-shell")

    assert len(result) == 1
    text = result[0].text
    assert "⧗ Not found" in text or "timeout" in text.lower()


def test_format_state_under_token_limit():
    """Test that output under limit is not condensed."""
    state = {
        "status": "running",
        "output": ["short", "output"],
    }

    result = pe.format_state_as_text(state, "test-tool")

    assert len(result) == 1
    text = result[0].text
    # Should not contain condensation message
    assert "Condensed" not in text
    assert "short" in text
    assert "output" in text


def test_format_state_over_token_limit():
    """Test that output over limit triggers condensation."""
    # Create output that exceeds MAX_SAFE_OUTPUT_TOKENS
    large_output = [f"Line {i}" * 100 for i in range(1000)]
    state = {
        "status": "running",
        "output": large_output,
    }

    result = pe.format_state_as_text(state, "test-tool")

    assert len(result) == 1
    text = result[0].text
    # Should contain condensation guidance
    assert "CONDENSED" in text or "omitted" in text.lower()


def test_format_state_preserves_structure():
    """Test that formatted output has correct structure."""
    state = {
        "status": "stopped",
        "returncode": 0,
        "output": ["Line 1"],
    }

    result = pe.format_state_as_text(state, "test-tool")

    assert len(result) == 1
    assert hasattr(result[0], 'text')
    assert hasattr(result[0], 'type')
    assert result[0].type == 'text'


def test_format_state_with_exception_info():
    """Test formatting with exception info in state."""
    state = {
        "status": "running",
        "output": ["Before error"],
        "exception": "ValueError: Invalid input",
    }

    result = pe.format_state_as_text(state, "test-tool")

    assert len(result) == 1
    text = result[0].text
    assert "ValueError: Invalid input" in text
