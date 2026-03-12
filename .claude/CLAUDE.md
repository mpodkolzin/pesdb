This is learning project for understanding database internals by building Columnar SQL Store from scratch

# Key Conventions

## Code Placement Philosophy

**Prefer extension code (db/) over core changes (postgres/):**
- Keep as much logic as possible in `db/` (our extension)
- Use hooks to integrate with PostgreSQL rather than modifying core
- Strike a balance based on change size:
  - **Significant logic** → Must go in `db/`
  - **Few lines or arg changes** → Can go in `postgres/`
- When in doubt, prefer hooks and extension code

## Project Structure

### Directory Layout
- **`src/{module}/`**: Implementation files (mirrored with include/)
- **`include/{module}/`**: Header files (mirrored with src/)
- **`include/common/`**: Common library utilities (prefer these over standard C++/PostgreSQL)

### Key Modules (src/ and /include/)
Core modules include: `common`, `rewriting`, `planner`, `column_store`, `join`, `shuffle_planner`, `shuffle_node`, `plugin`, `types`, `statistics_cache`, `instrumentation`, `logging`, and others.

### Build System
- **CMake**: Primary build system
  - Top-level: `CMakeLists.txt`
  - Unit tests: `tests/unit/CMakeLists.txt`
- **Makefiles**: PostgreSQL extension registration in `db/plugins/{plugin}/Makefile`

### Documentation Organization
- **`doc/design/{module}/`**: Module design documents (.adoc files with diagrams)
- **`doc/learnings/`**: Deep dives on specific topics (.adoc, .md files)
- **`doc/user/`**: User-facing documentation
- **`doc/team_processes/`**: Team workflows and processes

**Before creating ANY new file, ALWAYS search for similar existing files first**

For compilation/build/linking errors:
1. **ALWAYS** reproduce the error first by building WITHOUT the fix
2. Apply the fix
3. Build again to confirm the error is resolved
4. Report both results (error reproduced, then fixed)

For test failures:
1. **ALWAYS** run the test to confirm it fails before fixing
2. Apply the fix
3. Run the test again to confirm it passes
4. Report both results (failure reproduced, then fixed)

For code changes:
1. **ALWAYS** run relevant tests after making changes
2. **NEVER** skip verification even for "obvious" fixes
3. Use the MCP development environment for all verification

# MCP Development Environment
ALWAYS use the MCP development server for all software interactions:
- Fully containerized - the artifact and dev directory is mapped at SAME absolute paths in container
- Inside the container env vars like $DEV_DIR, $BUILD_DIR, $PG_BIN_DIR, $PG_BUILD_DIR, $ARTIFACTS_DIR exist for convenience
- Interactive sessions (psql, gdb, bash) for exploration; blocking tools for builds/tests
- Only one session type at a time, no parallel operations
- ALWAYS restart environment after code changes to pick up modifications

# Code Documentation
Prioritize self-documenting code through clear naming and structure over comments:
- Comments explain why, not what
- Refactor for clarity rather than adding explanatory comments
