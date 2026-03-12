"""Tests for output condensation with progressive filtering."""

import mcp_runner.output_condensation as oc


def test_classification():
    """Test line classification."""
    assert oc.classify_line("error: something bad happened").value == "error"
    assert oc.classify_line("FATAL: critical issue").value == "error"
    assert oc.classify_line("cannot connect to database").value == "error"
    assert oc.classify_line("warning: deprecated function").value == "warning"
    assert oc.classify_line("[123/456] Building...").value == "progress"
    assert oc.classify_line("Compiling src/main.c").value == "progress"
    assert oc.classify_line("just a normal line").value == "normal"


def test_token_counts():
    """Test that token counts are calculated."""
    classified = oc.classify_buffer(["Line 1\n", "Line 2\n", "Line 3\n"])

    assert all(hasattr(line, 'token_count') for line in classified)
    assert all(line.token_count > 0 for line in classified)


def test_progressive_filter_respects_budget():
    """Test that progressive filter stays under token budget."""
    large_output = []
    for i in range(10000):
        if i % 100 == 0:
            large_output.append("error: build failed at line {}\n".format(i))
        elif i % 50 == 0:
            large_output.append("warning: deprecated at line {}\n".format(i))
        else:
            large_output.append("Building file_{}.c with lots of options...\n".format(i))

    classified = oc.classify_buffer(large_output)
    budget = 15000
    _, tokens_used, stats = oc.apply_progressive_filter(classified, max_tokens=budget)

    assert tokens_used <= budget
    assert stats["total_tokens"] == tokens_used
    assert stats["budget"] == budget

def test_priority_order():
    """Test that errors are prioritized over warnings and normal lines."""
    output = []
    for i in range(1000):
        if i == 500:
            output.append("error: critical error\n")
        elif i == 501:
            output.append("warning: minor warning\n")
        else:
            output.append("normal line {}\n".format(i))

    classified = oc.classify_buffer(output)
    budget = 500
    filtered, _, stats = oc.apply_progressive_filter(classified, max_tokens=budget)

    error_kept = any(line.keep and "critical error" in line.text for line in filtered)
    assert error_kept
    assert stats["priority_levels"]["error"]["kept"] > 0


def test_large_output_many_errors():
    """Test that when errors exceed budget, some are kept."""
    output = []
    for i in range(1000):
        output.append("error: error number {} with lots of details here\n".format(i))

    classified = oc.classify_buffer(output)
    budget = 5000
    filtered, tokens_used, _ = oc.apply_progressive_filter(classified, max_tokens=budget)

    assert tokens_used <= budget

    kept_errors = sum(1 for line in filtered if line.keep and line.type == oc.LineType.ERROR)
    total_errors = sum(1 for line in classified if line.type == oc.LineType.ERROR)

    assert kept_errors < total_errors
    assert kept_errors > 0


def test_error_preservation_with_context():
    """Test that errors are kept with surrounding context lines."""
    output = []
    for i in range(100):
        if i == 50:
            output.append("error: critical failure\n")
        else:
            output.append("normal line {}\n".format(i))

    classified = oc.classify_buffer(output)
    budget = 10000
    context = 3
    filtered, _, _ = oc.apply_progressive_filter(classified, max_tokens=budget, context_lines=context)

    error_idx = 50
    for offset in range(-context, context + 1):
        idx = error_idx + offset
        if 0 <= idx < len(filtered):
            assert filtered[idx].keep


def test_bookends_always_kept():
    """Test that first and last lines are always kept."""
    output = ["FIRST\n"] + ["middle\n"] * 1000 + ["LAST\n"]

    classified = oc.classify_buffer(output)
    budget = 100
    filtered, _, _ = oc.apply_progressive_filter(classified, max_tokens=budget)

    assert filtered[0].keep
    assert filtered[-1].keep


def test_even_sampling_progress_lines():
    """Test that progress lines are sampled evenly when budget is limited."""
    # Create output with many progress lines
    output = []
    for i in range(200):
        output.append("[{}/200] Building file_{}.c\n".format(i, i))

    classified = oc.classify_buffer(output)
    # Set a budget that can't fit all progress lines
    budget = 2000
    filtered, _, stats = oc.apply_progressive_filter(classified, max_tokens=budget,
                                                     context_lines=0, bookend_size=5)

    # Get indices of kept progress lines (excluding bookends)
    bookend_size = 5
    kept_progress_indices = []
    for i, line in enumerate(filtered):
        if (line.keep and
            line.type == oc.LineType.PROGRESS and
            i >= bookend_size and
            i < len(filtered) - bookend_size):
            kept_progress_indices.append(i)

    if len(kept_progress_indices) > 1:
        # Check that indices are evenly spaced
        intervals = []
        for i in range(1, len(kept_progress_indices)):
            intervals.append(kept_progress_indices[i] - kept_progress_indices[i-1])

        # All intervals should be the same (even sampling)
        assert len(set(intervals)) == 1, \
            f"Intervals not uniform: {intervals}, indices: {kept_progress_indices}"

        # Check that samples span the range
        first_kept = kept_progress_indices[0]
        last_kept = kept_progress_indices[-1]
        assert last_kept - first_kept > 50, \
            "Sampled lines should span across the range"


def test_even_sampling_with_context():
    """Test even sampling preserves context around sampled lines."""
    # Create output with errors at regular intervals
    output = []
    error_indices = []
    for i in range(100):
        if i % 10 == 0:
            output.append("error: error at line {}\n".format(i))
            error_indices.append(i)
        else:
            output.append("normal line {}\n".format(i))

    classified = oc.classify_buffer(output)
    budget = 1000
    context = 2
    filtered, _, _ = oc.apply_progressive_filter(classified, max_tokens=budget,
                                                 context_lines=context, bookend_size=5)

    # Find which errors were kept (excluding bookends)
    bookend_size = 5
    kept_error_indices = [i for i in error_indices
                          if i >= bookend_size and
                          i < len(filtered) - bookend_size and
                          filtered[i].keep]

    # For each kept error, verify its context is also kept
    for error_idx in kept_error_indices:
        for offset in range(-context, context + 1):
            ctx_idx = error_idx + offset
            if 0 <= ctx_idx < len(filtered):
                assert filtered[ctx_idx].keep, \
                    f"Context line at {ctx_idx} not kept for error at {error_idx}"


def test_sampling_interval_calculation():
    """Test that sampling uses the smallest interval that fits budget."""
    # Create uniform output with long lines to force sampling
    output = []
    for i in range(100):
        output.append("[{}/100] Progress line {} with many compiler flags and options\n".format(i, i))

    classified = oc.classify_buffer(output)

    # First test: generous budget should keep more lines
    budget_generous = 5000
    filtered_generous, _, stats_generous = oc.apply_progressive_filter(
        classified, max_tokens=budget_generous, context_lines=0, bookend_size=5)

    kept_generous = sum(1 for line in filtered_generous if line.keep)

    # Second test: tight budget should keep fewer lines
    classified2 = oc.classify_buffer(output)
    budget_tight = 1000
    filtered_tight, _, stats_tight = oc.apply_progressive_filter(
        classified2, max_tokens=budget_tight, context_lines=0, bookend_size=5)

    kept_tight = sum(1 for line in filtered_tight if line.keep)

    # Generous budget should keep more lines than tight budget
    assert kept_generous > kept_tight, \
        f"Generous budget kept {kept_generous}, tight budget kept {kept_tight}"
