#!/bin/bash
#
# Test runner for MCP server tests using pytest
#
# This script runs all tests with the correct Python environment
#

set -euo pipefail

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_DIR="$(cd "$RUNNER_DIR/.." && pwd)"

echo "Repo directory: $REPO_DIR"
echo "Activating venv..."

# Activate virtual environment
source "$RUNNER_DIR/venv/bin/activate"

echo "Python: $(which python3)"
echo "Python version: $(python3 --version)"
echo ""

# Run pytest from repo root so runner/runner path works
# -v: verbose
# -s: show print statements
# --tb=short: shorter traceback format
cd "$REPO_DIR"
python3 -m pytest runner/tests/ -v -s --tb=short
