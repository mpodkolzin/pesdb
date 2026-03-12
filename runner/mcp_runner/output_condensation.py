"""
Output condensation and intelligent filtering for MCP server.

Handles classification of output lines and progressive filtering
to stay within token budgets while preserving critical information.
"""

import re
import sys
from enum import Enum
from typing import List, Dict
from mcp_runner.constants import *

# Classification system for intelligent output filtering
class LineType(Enum):
    ERROR = "error"
    WARNING = "warning"
    FAILURE = "failure"
    SUCCESS = "success"
    PROGRESS = "progress"
    NORMAL = "normal"


class ClassifiedLine:
    """Represents a classified line with filtering metadata."""
    def __init__(self, index, text, type, keep=False):
        self.index = index
        self.text = text
        self.type = type
        self.keep = keep
        self.token_count = count_tokens(text)


CLASSIFICATION_PATTERNS = {
    LineType.ERROR: [
        r'(?i)\berror\b',
        r'(?i)\bfatal\b',
        r'(?i)cannot\b',
        r'failed.*with',
        r'compilation.*failed',
    ],
    LineType.WARNING: [
        r'(?i)\bwarning\b',
        r'(?i)\bcaution\b',
    ],
    LineType.FAILURE: [
        r'(?i)\bfail(ed|ure)\b',
        r'exit code.*[^0]',
        r'Build.*FAILED',
    ],
    LineType.SUCCESS: [
        r'(?i)success',
        r'(?i)complet(ed|e)',
        r'exit code.*0',
        r'Build.*succeeded',
    ],
    LineType.PROGRESS: [
        r'\[\d+/\d+\]',
        r'\d+%',
        r'Building',
        r'Compiling',
        r'Linking',
    ],
}

# Priority order for progressive filtering (most to least critical)
PRIORITY_ORDER = [
    LineType.ERROR,
    LineType.FAILURE,
    LineType.WARNING,
    LineType.SUCCESS,
    LineType.PROGRESS,
    LineType.NORMAL,
]


def count_tokens(text: str) -> int:
    """Count tokens (words) in text using simple whitespace splitting."""
    return len(re.split(r"[^\w]", text))


def classify_line(line: str) -> LineType:
    """Classify a single line by pattern matching."""
    for line_type, patterns in CLASSIFICATION_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, line):
                return line_type
    return LineType.NORMAL


def classify_buffer(buffer: List[str]) -> List[ClassifiedLine]:
    """Classify all lines in buffer."""
    return [
        ClassifiedLine(index=i, text=line, type=classify_line(line))
        for i, line in enumerate(buffer)
    ]


def count_by_type(classified: List[ClassifiedLine]) -> Dict:
    """Count lines by type."""
    counts = {t: 0 for t in LineType}
    for line in classified:
        counts[line.type] += 1
    return counts


def save_full_output(buffer: List[str], tool_name: str):
    """Save complete output to file and return filepath."""
    if not ARTIFACTS_OUTPUT_DIR:
        return None

    try:
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_tool_name = re.sub(r'[^\w\-]', '_', tool_name) if tool_name else "output"
        filename = f"{timestamp}_{safe_tool_name}.txt"
        filepath = f"{ARTIFACTS_OUTPUT_DIR}/{filename}"
        with open(filepath, 'w') as f:
            f.writelines(buffer)
        return filepath
    except Exception as e:
        print(f"Error saving full output: {e}", file=sys.stderr)
        return None


def apply_progressive_filter(classified: List[ClassifiedLine], max_tokens: int, context_lines: int = 3, bookend_size: int = 20):
    """
    Progressively add line types until we hit the token budget.
    Uses even sampling when budget doesn't allow all lines of a type.
    Returns (filtered_lines, tokens_used, stats_dict).

    Strategy:
    1. Always keep bookends (first/last N lines)
    2. For each priority level (ERROR, FAILURE, WARNING, etc.):
       - Collect all lines of that type
       - If all fit in budget, keep all
       - If not, use even sampling (e.g., if 30% fit, take every 3rd line)
       - Stop when budget exhausted
    """
    total_lines = len(classified)

    # Step 1: Mark bookends and calculate their tokens
    tokens_used = 0
    bookend_indices = set()

    for i in range(min(bookend_size, total_lines)):
        classified[i].keep = True
        bookend_indices.add(i)
        tokens_used += classified[i].token_count

    for i in range(max(0, total_lines - bookend_size), total_lines):
        if i not in bookend_indices:  # Don't double-count overlap
            classified[i].keep = True
            bookend_indices.add(i)
            tokens_used += classified[i].token_count

    # Track stats for each priority level
    stats = {
        "bookends_tokens": tokens_used,
        "priority_levels": {},
    }

    # Step 2: Progressively add line types by priority with even sampling
    for priority_type in PRIORITY_ORDER:
        # Find all lines of this type that aren't already kept
        type_indices = [i for i, line in enumerate(classified)
                       if line.type == priority_type and not line.keep]

        if not type_indices:
            continue

        # Calculate total tokens needed for all lines of this type + context
        lines_with_context = set()
        for i in type_indices:
            lines_with_context.add(i)
            for offset in range(-context_lines, context_lines + 1):
                idx = i + offset
                if 0 <= idx < total_lines:
                    lines_with_context.add(idx)

        # Only consider lines not already kept
        new_lines = [idx for idx in sorted(lines_with_context) if not classified[idx].keep]
        total_tokens_needed = sum(classified[idx].token_count for idx in new_lines)
        remaining_budget = max_tokens - tokens_used

        kept_count = 0
        kept_tokens = 0

        if total_tokens_needed <= remaining_budget:
            # We can fit everything
            for idx in new_lines:
                classified[idx].keep = True
                tokens_used += classified[idx].token_count
                kept_tokens += classified[idx].token_count
            kept_count = len(type_indices)
        else:
            # Use even sampling - find the largest interval that fits in budget
            for sample_interval in range(1, len(type_indices) + 1):
                # Sample the primary lines (not context) evenly
                sampled_primary = type_indices[::sample_interval]

                # Collect sampled lines + their context
                sampled_with_context = set()
                for i in sampled_primary:
                    sampled_with_context.add(i)
                    for offset in range(-context_lines, context_lines + 1):
                        idx = i + offset
                        if 0 <= idx < total_lines:
                            sampled_with_context.add(idx)

                # Only count new lines
                new_sampled = [idx for idx in sorted(sampled_with_context) if not classified[idx].keep]
                sampled_tokens = sum(classified[idx].token_count for idx in new_sampled)

                if sampled_tokens <= remaining_budget:
                    # This sampling interval fits
                    for idx in new_sampled:
                        classified[idx].keep = True
                        tokens_used += classified[idx].token_count
                        kept_tokens += classified[idx].token_count
                    kept_count = len(sampled_primary)
                    break

        stats["priority_levels"][priority_type.value] = {
            "attempted": len(type_indices),
            "kept": kept_count,
            "tokens": kept_tokens,
        }

        # If we're at or over budget, stop trying more types
        if tokens_used >= max_tokens:
            break

    stats["total_tokens"] = tokens_used
    stats["budget"] = max_tokens
    stats["context_lines"] = context_lines

    return classified, tokens_used, stats


def format_condensed_output(classified: List[ClassifiedLine]) -> str:
    """
    Format filtered output with line numbers.
    Preserves chronological order.
    """
    parts = []

    for line in classified:
        if line.keep:
            # Add the line with index for reference
            parts.append(f"{line.index:6d} | {line.text}")

    return "".join(parts)


def condense_output(buffer: List[str], tool_name: str, max_tokens: int = 18000, bookend_size: int = 20) -> Dict:
    """
    Condense output intelligently while preserving order and context.
    Uses progressive filtering with actual token counts.
    Returns dict with condensed output and metadata.
    """
    # Always save full output
    full_file = save_full_output(buffer, tool_name)

    # Classify all lines
    classified = classify_buffer(buffer)

    # Count by type
    counts = count_by_type(classified)

    # Apply progressive filter
    filtered, tokens_used, filter_stats = apply_progressive_filter(classified, max_tokens=max_tokens, bookend_size=bookend_size)

    # Format condensed output
    condensed_text = format_condensed_output(filtered)

    # Count what we kept
    kept_count = sum(1 for line in filtered if line.keep)

    # Build statistics summary
    stats_parts = [
        f"Total lines: {len(buffer)}",
        f"Condensed to: {kept_count} lines ({kept_count*100//len(buffer) if len(buffer) > 0 else 0}%)",
        f"Token budget: {tokens_used} / {filter_stats['budget']} tokens used",
        "",
        "Line type distribution:",
    ]
    for line_type, count in counts.items():
        if count > 0:
            stats_parts.append(f"  • {line_type.value}: {count}")

    stats_parts.extend([
        "",
        "Progressive filtering applied (by priority):",
        f"  • Bookends: first/last 20 lines ({filter_stats['bookends_tokens']} tokens)",
    ])

    # Show what was kept for each priority level
    for priority_type in PRIORITY_ORDER:
        level_stats = filter_stats["priority_levels"].get(priority_type.value, {})
        if level_stats.get("attempted", 0) > 0:
            kept = level_stats.get("kept", 0)
            attempted = level_stats.get("attempted", 0)
            tokens = level_stats.get("tokens", 0)
            stats_parts.append(f"  • {priority_type.value}: {kept}/{attempted} kept ({tokens} tokens)")

    return {
        "condensed": condensed_text,
        "stats": "\n".join(stats_parts),
        "full_file": full_file,
        "counts": counts,
        "kept_count": kept_count,
        "tokens_used": tokens_used,
    }
