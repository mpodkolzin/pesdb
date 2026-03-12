"""
MCP Server entry point for RaptorDB Development Environment.

This module provides the paver task that initializes and runs the MCP server.
"""

import asyncio
import os
from paver.easy import task

# Import the server runner
from mcp_runner import run_server


@task
def mcp_server(options):
    """
    Start the MCP server for the RaptorDB containerized development environment.

    This task:
    - Sets global configuration (container name, artifacts directory)
    - Runs the MCP server using stdio transport
    """
    # Import modules to set module-level globals
    import mcp_runner.constants as constants
    import mcp_runner.output_condensation as output_condensation
    import mcp_runner.task_resolver as task_resolver

    # Set global configuration
    constants.CONTAINER_NAME = options.get('mcp_container', 'mcp_runner')
    constants.ARTIFACTS_OUTPUT_DIR = f'{options.paths.artifacts}/output/mcp'
    task_resolver.CONTAINER_NAME = constants.CONTAINER_NAME
    output_condensation.ARTIFACTS_OUTPUT_DIR = constants.ARTIFACTS_OUTPUT_DIR

    # Ensure artifacts directory exists
    os.makedirs(constants.ARTIFACTS_OUTPUT_DIR, exist_ok=True)

    # Run the server
    asyncio.run(run_server())
