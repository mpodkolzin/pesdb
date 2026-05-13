# PesDB - Learning Database Internals Through Building

This is a **learning project** to understand database internals by building a columnar SQL store from scratch. The focus is on learning, not production code.

## Learning Goals

1. **Refresh C++ knowledge** - Explain tricky concepts, idioms, and modern C++ patterns
2. **Master database internals** - Understand how databases actually work by implementing them
3. **Build working columnar storage** - Practical implementation that validates understanding

## Working Approach

### Design First, Then Code

**ALWAYS follow this sequence:**

1. **Propose Design** - Explain the approach before writing code
   - "Here's how I think we should implement X..."
   - Explain the database concept/algorithm
   - Call out interesting tradeoffs or design choices
   
2. **Q&A Discussion** - Ask and answer questions
   - Why this approach over alternatives?
   - What are the implications?
   - What could go wrong?
   
3. **Document Decision** - Store the design in `doc/design/{module}/`
   - Capture the "why" not just the "what"
   - Include alternatives considered and rejected
   
4. **Implement** - Write the code with understanding

**NEVER jump straight to code.** If I ask for implementation without design, push back: "Let's design this first - here's what I'm thinking..."

### Explain C++ Concepts

When encountering tricky C++ (RAII, move semantics, templates, smart pointers, etc.):

1. **Explain the concept** - What it is, why it exists, how it works
2. **Show the pattern** - Code example in context
3. **Store for reference** - Save to `doc/learnings/cpp/`

Don't assume I remember everything. If it's non-trivial, explain it.

### Explain Database Concepts

When implementing database features (page layouts, buffer pools, WAL, indexes):

1. **Explain the theory** - How do real databases do this?
2. **Reference papers/systems** - "PostgreSQL does X, SQLite does Y"
3. **Justify our approach** - Why are we doing it this way?
4. **Store insights** - Save to `doc/learnings/database/`

Build up a knowledge base as we go.

## Documentation Organization

### Design Documents (`doc/design/{module}/`)
- **Purpose**: Design decisions for specific modules
- **Format**: `.md` or `.adoc` files
- **Content**: Problem -> Approach -> Alternatives -> Decision rationale
- **When**: Before implementing a new module/feature

### Learning Notes (`doc/learnings/`)
- **`doc/learnings/cpp/`**: C++ concepts, patterns, gotchas
- **`doc/learnings/database/`**: Database theory, algorithms, papers
- **Format**: `.md` files, one concept per file
- **Content**: Explanation -> Example -> Why it matters
- **When**: Whenever we learn something worth remembering

### User Documentation (`doc/user/`)
- How to use the database (SQL, features, etc.)

### Team Processes (`doc/team_processes/`)
- Team workflows and processes

## Project Structure

### Directory Layout
- **`src/{module}/`**: Implementation files (mirrored with include/)
- **`include/{module}/`**: Header files (mirrored with src/)
- **`include/common/`**: Common library utilities (prefer these over standard C++/PostgreSQL)

### Key Modules (src/ and /include/)
Core modules include: `common`, `rewriting`, `planner`, `column_store`, `join`, `shuffle_planner`, `shuffle_node`, `plugin`, `types`, `statistics_cache`, `instrumentation`, `logging`, `storage`, `buffer_pool`, `page`, `wal`, and others.

### Build System
- **CMake**: Primary build system
  - Top-level: `CMakeLists.txt`
  - Unit tests: `tests/unit/CMakeLists.txt`
- **Makefiles**: PostgreSQL extension registration in `db/plugins/{plugin}/Makefile`

**Before creating ANY new file, ALWAYS search for similar existing files first**

## MCP Development Environment

ALWAYS use the MCP development server for all software interactions:
- Fully containerized - the artifact and dev directory is mapped at SAME absolute paths in container
- Inside the container env vars like $DEV_DIR, $BUILD_DIR, $PG_BIN_DIR, $PG_BUILD_DIR, $ARTIFACTS_DIR exist for convenience
- Interactive sessions (psql, gdb, bash) for exploration; blocking tools for builds/tests
- Only one session type at a time, no parallel operations
- ALWAYS restart environment after code changes to pick up modifications

## Verification Requirements

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

## Code Placement Philosophy

**Prefer extension code (db/) over core changes (postgres/):**
- Keep as much logic as possible in `db/` (our extension)
- Use hooks to integrate with PostgreSQL rather than modifying core
- Strike a balance based on change size:
  - **Significant logic** -> Must go in `db/`
  - **Few lines or arg changes** -> Can go in `postgres/`
- When in doubt, prefer hooks and extension code

## Code Documentation

Prioritize self-documenting code through clear naming and structure over comments:
- Comments explain why, not what
- Refactor for clarity rather than adding explanatory comments

## Teaching Style

- **Explain before doing** - Design discussions, concept explanations
- **Ask questions** - "What if we did X instead?"
- **Show alternatives** - "Real databases do A, B, or C"
- **Learn from mistakes** - Bugs and errors are teaching moments
- **Build incrementally** - Start simple, add complexity as we understand

**Remember: This is about learning, not shipping. Understanding matters more than perfect code.**
