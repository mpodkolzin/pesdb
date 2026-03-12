"""
MCP Server for RaptorDB Development Environment.

This package provides modular components for the MCP (Model Context Protocol) server
that enables AI interactions with the RaptorDB containerized development environment.

Key modules:
- constants: Configuration and constants
- output_condensation: Intelligent output filtering and token management
- task_resolver: Task resolution and command building
- session_manager: Session lifecycle management
- process_executor: Process execution and output formatting
- mcp_handlers: MCP protocol request handlers
"""

# Lazy import to avoid loading MCP library dependencies when importing sub-modules
def run_server():
    """Import and run the MCP server (lazy import to avoid loading MCP library prematurely)."""
    from mcp_runner.mcp_handlers import run_server as _run_server
    return _run_server()

# Main entry point for the server
__all__ = ['run_server']
