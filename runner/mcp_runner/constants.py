"""
Constants and configuration for the MCP server.
"""

# Timeout configurations
KILL_GRACE_SECONDS = 10
OUTPUT_FLUSH_WAIT_SECONDS = 5.0
WAIT_TIMEOUT = 180
MAX_WAIT_TIMEOUT = 900

# Token limits
MAX_SAFE_OUTPUT_TOKENS = 2000  # Safe margin for MCP output to Claude Code
MAX_OUTPUT_BOOKEND_SIZE = 5

# Container configuration
CONTAINER_NAME = "mcp_runner"
ARTIFACTS_OUTPUT_DIR = None  # Set by mcp_server task

# MCP server description
DESCRIPTION = """
MCP server for the RaptorDB containerized development environment.

**Environment:**
- Fully containerized - NEVER use normal shell commands
- Git repo, artifacts, and build dirs mapped at SAME absolute paths in container

**Interactive Sessions:**
- Persistent shells (psql, gdb, bash) for exploration and debugging
- Use restart_environment to reset everything cleanly
"""

# Interactive session messages
INTERACTIVE_STOPPED = "WARNING: The interactive session has been stopped.\n"
INTERACTIVE_RESTARTED = "WARNING: The interactive session has been restarted.\n"

# JSON Schema for interactive tool input
INTERACT_INPUT = {
    "type": "object",
    "properties": {
        "input": {
            "type": "array",
            "items": {"type": "string"},
            "description": (
                "Input to send to the async process (each element is one write/line)"
                "Use empty array [] to poll for new output without sending input."
            )
        },
        "timeout": {
            "type": "number",
            "description": (
                "Maximum seconds to wait for prompt detection (default: 5.0). "
                "This is a safety timeout - if the tool prompt is detected the tool will return directly."
            ),
            "default": 5.0
        },
        "restart": {
            "type": "boolean",
            "description": (
                "Force kill and restart this specific interactive tool (default: false). "
                "Use when the tool is stuck or unresponsive."
            ),
            "default": False
        }
    },
    "required": ["input", "timeout"]
}

# JSON Schema for restart tool input
RESTART_INPUT = {
    "type": "object",
    "properties": {},
    "required": []
}
