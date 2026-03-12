"""
Decorators for MCP functionality.
"""

def mcp_enabled(func=None, **kwargs):
    """
    Decorator to mark a function as MCP enabled.

    Optional:
      - interactive: if True, sets a tag `mcp_interactive = True` on the function so
        callers can detect intended interactive/async usage later. No wrapping is performed.
      - async_requires: canonical task name (e.g., 'db.client') that must be started async first
      - prompt_pattern: regex pattern to detect when a command has completed (prompt is visible)
      - cmd: command to execute (list of strings)
    """

    interactive = bool(kwargs.get("interactive", False))
    async_requires = kwargs.get("async_requires", None)
    prompt_pattern = kwargs.get("prompt_pattern", None)

    def decorator(f):
        # Always tag as mcp_enabled
        setattr(f, "mcp_enabled", True)
        if interactive:
            setattr(f, "mcp_interactive", True)
        if async_requires:
            setattr(f, "mcp_async_requires", async_requires)
        if prompt_pattern:
            setattr(f, "mcp_prompt_pattern", prompt_pattern)
        return f

    # Handle both @mcp_enabled and @mcp_enabled() syntax
    if func is None:
        return decorator
    return decorator(func)
