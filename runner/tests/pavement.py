"""
Test pavement with simple tasks for integration testing.
"""

import sys
from pathlib import Path

# Add parent directory to import mcp_runner
sys.path.insert(0, str(Path(__file__).parent.parent))

from paver.easy import task
from mcp_runner.decorators import mcp_enabled


@task
@mcp_enabled(interactive=True, prompt_pattern=r'PROMPT###')
def echo_shell():
    """Interactive shell that echoes input"""
    print("PROMPT###", flush=True)

    while True:
        try:
            line = input()
            if line.strip() == "EXIT":
                break
            print(line, flush=True)
            print("PROMPT###", flush=True)
        except EOFError:
            break


@task
@mcp_enabled(interactive=True, prompt_pattern=r'READY>')
def stateful_shell():
    """Interactive shell that maintains counter state"""
    counter = 0
    print("Stateful shell started", flush=True)
    print("READY>", flush=True)

    while True:
        try:
            line = input()
            if line.strip() == "EXIT":
                break
            elif line.strip() == "INCREMENT":
                counter += 1
                print("Counter: {}".format(counter), flush=True)
            elif line.strip() == "GET":
                print("Counter: {}".format(counter), flush=True)
            elif line.strip() == "RESET":
                counter = 0
                print("Counter reset", flush=True)
            else:
                print("Unknown command: {}".format(line), flush=True)
            print("READY>", flush=True)
        except EOFError:
            break


@task
@mcp_enabled(interactive=True, prompt_pattern=r'>>>')
def python_style_shell():
    """Interactive shell with Python-style prompt"""
    print("Python-style shell ready", flush=True)
    print(">>>", flush=True)

    while True:
        try:
            line = input()
            if line.strip() == "exit":
                break
            print("You said: {}".format(line), flush=True)
            print(">>>", flush=True)
        except EOFError:
            break


@task
@mcp_enabled(interactive=True, prompt_pattern=r'DEP_READY>')
def dependency_task():
    """Interactive task that is a dependency for other tasks"""
    print("DEPENDENCY_EXECUTED", flush=True)
    print("DEP_READY>", flush=True)

    while True:
        try:
            line = input()
            if line.strip() == "EXIT":
                break
            print("Dependency got: {}".format(line), flush=True)
            print("DEP_READY>", flush=True)
        except EOFError:
            break


@task
@mcp_enabled(interactive=True, async_requires='dependency_task', prompt_pattern=r'MAIN_READY>')
def task_with_needs():
    """Interactive task that requires dependency_task to run first"""
    print("MAIN_TASK_EXECUTED", flush=True)
    print("MAIN_READY>", flush=True)

    while True:
        try:
            line = input()
            if line.strip() == "EXIT":
                break
            print("Main got: {}".format(line), flush=True)
            print("MAIN_READY>", flush=True)
        except EOFError:
            break


@task
@mcp_enabled()
def blocking_test():
    """Blocking test tool that prints and exits"""
    print("Blocking tool executed")


@task
@mcp_enabled()
def blocking_large_output():
    """Blocking tool that produces large output to test buffer flushing"""
    # Produce enough output to test buffer flushing (simulates build-clean)
    print("START_MARKER", flush=True)
    for i in range(100):
        print("Line {}: {}".format(i, "x" * 100), flush=True)
    print("END_MARKER", flush=True)


@task
@mcp_enabled(interactive=True, prompt_pattern=r'SHELL>')
def pty_echo_shell():
    """Interactive shell that uses script command to create PTY with echo.

    This reproduces the actual bug scenario: PTY echo + application echo
    racing and corrupting output. Without 'stty -echo', input would be
    echoed twice causing corruption.

    The fix is in the shell script which runs 'stty -echo' before the
    echo loop, disabling PTY echo to prevent double-echo corruption.
    """
    import subprocess
    import sys
    import os

    # Create a temporary shell script that echoes input (like psql -a)
    # Handles PTY echo just like psql -a does
    script_content = '''#!/bin/bash
# Disable PTY echo to prevent double-echo with the echo loop below
stty -echo 2>/dev/null || true
echo "Ready"
echo "SHELL>"
while IFS= read -r line; do
    # Echo the input (simulating psql -a)
    echo "$line"
    echo "SHELL>"
done
'''

    # Write to temp file
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as f:
        f.write(script_content)
        script_file = f.name

    try:
        os.chmod(script_file, 0o755)
        # Use script command like db.client does
        # macOS script syntax: script -q output_file command [args...]
        # Linux script syntax: script -q output_file -c "command"
        if sys.platform == 'darwin':
            # macOS: script -q /dev/null command
            subprocess.run(['script', '-q', '/dev/null', script_file])
        else:
            # Linux: script -q /dev/null -c command
            subprocess.run(['script', '-q', '/dev/null', '-c', script_file])
    finally:
        os.unlink(script_file)
