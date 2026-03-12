"""
Process execution and output formatting.

Handles formatting of execution state into MCP TextContent
with intelligent output condensation.
"""

from typing import Any, Dict
import mcp.types as types

from mcp_runner.constants import *
from mcp_runner.output_condensation import *


def format_state_as_text(state: Dict[str, Any], tool_name: str = "") -> list[types.TextContent]:
    """Convert state dict to MCP TextContent with intelligent condensation."""
    parts = []

    # Header with tool and status/prompt
    parts.append("═══════════════════════════════════════════════════════")
    parts.append(f"TOOL: {tool_name}")
    
    # Show prompt status if present, otherwise show regular status
    if "reached_prompt" in state:
        if state["reached_prompt"]:
            parts.append("PROMPT: ✓ detected - ready for next command")
        else:
            parts.append("PROMPT: ⧗ Not found, timeout reached - command still running")
            parts.append(f"STATUS: {state.get('status', 'unknown')}")
    else:
        parts.append(f"STATUS: {state.get('status', 'unknown')}")
    
    # Add exception if present
    if "exception" in state and state["exception"]:
        parts.append(f"EXCEPTION: {state['exception']}")
    
    # Add error if present
    if "error" in state:
        parts.append(f"ERROR: {state['error']}")
        if "stacktrace" in state:
            parts.append("STACKTRACE:")
            parts.append("".join(state["stacktrace"]))
    
    parts.append("═══════════════════════════════════════════════════════")
    parts.append("")

    # Handle output section with intelligent condensation
    if "output" in state:
        output_lines = state["output"]
        if output_lines:
            # Calculate total tokens
            total_tokens = sum(count_tokens(line) for line in output_lines)

            # Decide: condense or show full?
            if total_tokens > MAX_SAFE_OUTPUT_TOKENS:
                # Use intelligent condensation
                condensed = condense_output(output_lines, tool_name, max_tokens=MAX_SAFE_OUTPUT_TOKENS, bookend_size=MAX_OUTPUT_BOOKEND_SIZE)

                parts.append("⚠️  OUTPUT CONDENSED - Full output saved for analysis")
                parts.append(f"📄 Full output: {condensed['full_file']}")
                parts.append("")
                parts.append("⚠️  IMPORTANT: Do not make hasty conclusions from condensed output below.")
                parts.append("    The condensed view shows errors/warnings/bookends with context.")
                parts.append("    Lines are chronologically ordered with line numbers.")
                parts.append("    If diagnosis is unclear, analyze the full output file")
                parts.append("")
                parts.append("═══════════════════════════════════════════════════════")
                parts.append("CONDENSED OUTPUT (chronological, with line numbers):")
                parts.append("═══════════════════════════════════════════════════════")
                parts.append("")
                parts.append(condensed['condensed'])
                parts.append("")
                parts.append("═══════════════════════════════════════════════════════")
                parts.append("END OF CONDENSED OUTPUT")
                parts.append("═══════════════════════════════════════════════════════")

            else:
                # Under limit - show full output
                parts.append("Output:")
                parts.append("".join(output_lines))
        else:
            parts.append("No output")
    else:
        parts.append("No output")

    text = "\n".join(parts)

    return [types.TextContent(type="text", text=text)]
