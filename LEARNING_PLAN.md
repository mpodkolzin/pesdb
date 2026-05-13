# pesdb Learning + Development Plan

**Project**: Building a columnar SQL store to understand database internals
**Approach**: Design first, then code. Explain concepts, build understanding.
**Current Status**: Phase 1 - Storage Foundation (nearly complete)

## Key Principles (READ THIS FIRST)

**DESIGN FIRST, THEN CODE**
- Never jump straight to implementation
- Always discuss approach, alternatives, tradeoffs
- Document design in `doc/design/{module}/` before coding
- If I ask for code without design, push back: "Let's design this first"

**EXPLAIN AS WE BUILD**
- C++ concepts: RAII, move semantics, smart pointers -> explain + document in `doc/learnings/cpp/`
- Database concepts: WAL, MVCC, B+trees -> explain theory + document in `doc/learnings/database/`
- Reference real databases: "PostgreSQL does X, SQLite does Y"

**LEARN FROM MISTAKES**
- Errors are teaching moments
- Explain what broke and why
- Document gotchas for future reference

**BUILD INCREMENTALLY**
- Simple version first, optimize later
- One concept at a time
- It's OK to refactor as understanding grows

---

## Learning Philosophy

**How We Work:**
1. **Understand before building** - Design discussions come first
2. **Explain, then implement** - Teach the concept, then write the code
3. **Build incrementally** - Simple version first, add complexity as understanding grows
4. **Learn from errors** - Bugs are teaching moments, not failures
5. **Document insights** - Capture learnings in `doc/learnings/` and `doc/design/`

**Questions We Ask:**
- "What database concept does this teach?"
- "How do real databases (PostgreSQL, SQLite, DuckDB) solve this?"
- "What are the tradeoffs of different approaches?"
- "What C++ pattern is appropriate here and why?"
- "What could go wrong?"

**Success Looks Like:**
- Understanding WHY, not just WHAT
- Being able to explain concepts to others
- Building working code that validates understanding
- A growing knowledge base in `doc/learnings/`

**Avoid:**
- Jumping to code without design
- Copying patterns without understanding them
- Over-engineering before grasping fundamentals
- Skipping verification (tests, builds)

---

## Phase 1: Storage Foundation (CURRENT)

**Goal**: Build the foundation for persistent storage

### Completed
- [x] Page abstraction (in-memory page representation)
- [x] Disk Manager (page allocation, read/write)
- [x] Buffer Pool Manager (caching layer)

### In Progress
- [ ] Fix BufferPool LRU eviction bug (SimpleLRUEviction test failing)
- [ ] Complete WAL (Write-Ahead Logging) design

### Learning Outcomes
- Memory layout and cache-friendly data structures
- Thread safety with shared_mutex (readers-writer locks)
- RAII patterns for resource management
- Page lifecycle (fetch, pin, unpin, flush)
- LRU eviction policies
- Dirty page management
- Write-before-evict pattern for durability

### Key Files
```
include/columnar_db/storage/
  page.h                    -> In-memory page representation
  disk_manager.h            -> Raw file I/O layer
  buffer_pool_manager.h     -> Caching layer

src/storage/
  disk_manager.cpp          -> Implemented
  buffer_pool_manager.cpp   -> Implemented (has bug)

doc/design/storage/
  page.md                   -> Page design
  disk-manager.md           -> Disk manager design
  buffer_pool_manager.md    -> Buffer pool design
  file-layout.md            -> On-disk format
```

---

## Phase 2: Recovery & Durability

**Goal**: Ensure data survives crashes

### Components to Build
1. **WAL (Write-Ahead Logging)**
   - Log record format
   - Log manager (append-only log)
   - Log buffer management
   - Force-log-at-commit protocol

2. **Recovery Manager**
   - REDO log replay
   - Checkpoint mechanism
   - Crash recovery process

### What You'll Learn
- Write-ahead logging protocol
- REDO vs UNDO logs
- Checkpoint strategies
- Crash recovery algorithms
- Log sequence numbers (LSN)
- Force vs no-force, steal vs no-steal policies

### Estimated Time: 2-3 weeks

### Key References
- ARIES paper (recovery algorithm)
- PostgreSQL WAL implementation
- doc/design/wal/log_manager.md (started)

---

## Phase 3: Column Store Fundamentals

**Goal**: Implement columnar storage format

### Components to Build
1. **Column Layout**
   - Column chunk format
   - Compression (basic run-length encoding)
   - Dictionary encoding for strings

2. **Tuple Reconstruction**
   - Materialize row from columns
   - Column scan operators
   - Projection pushdown

3. **Insert/Update/Delete**
   - Delta stores for modifications
   - Merge operations
   - Versioning strategy

### What You'll Learn
- Row vs column storage trade-offs
- Compression techniques
- Vectorized execution basics
- Late materialization
- Update strategies for immutable columns

### Estimated Time: 3-4 weeks

---

## Phase 4: Query Execution Basics

**Goal**: Execute simple SQL queries

### Components to Build
1. **SQL Parser** (use Hyrise SQL - already integrated)
   - Parse SELECT/INSERT/UPDATE/DELETE
   - Build parse tree

2. **Simple Planner**
   - Convert parse tree to logical plan
   - No optimization yet (just correctness)

3. **Executor**
   - Table scan operator
   - Filter (WHERE clause)
   - Projection (SELECT columns)
   - Insert/Update/Delete executors

### What You'll Learn
- SQL parsing basics
- Logical vs physical plans
- Iterator model (Volcano-style)
- Predicate evaluation
- Tuple-at-a-time vs vectorized execution

### Estimated Time: 2-3 weeks

---

## Phase 5: Transactions & Concurrency

**Goal**: Support concurrent transactions

### Components to Build
1. **Transaction Manager**
   - Transaction IDs
   - Begin/Commit/Abort
   - Transaction context

2. **MVCC (Multi-Version Concurrency Control)**
   - Tuple versioning
   - Visibility rules
   - Garbage collection

3. **Lock Manager** (optional, if not full MVCC)
   - Row-level locking
   - Deadlock detection

### What You'll Learn
- ACID properties
- MVCC implementation
- Snapshot isolation
- Visibility determination
- Version chain management
- Deadlock prevention strategies

### Estimated Time: 3-4 weeks

---

## Phase 6: Indexing

**Goal**: Speed up lookups

### Components to Build
1. **B+ Tree Index**
   - Insert/Delete/Search
   - Concurrent access (latch crabbing)
   - Split/merge operations

2. **Index Manager**
   - Create/drop index
   - Index selection
   - Multi-column indexes

### What You'll Learn
- B+ tree internals
- Index concurrent access patterns
- Latch crabbing protocol
- Index maintenance during updates
- Index scan vs table scan trade-offs

### Estimated Time: 2-3 weeks

---

## Phase 7: Query Optimization

**Goal**: Generate efficient query plans

### Components to Build
1. **Statistics**
   - Table/column statistics
   - Histograms
   - Cardinality estimation

2. **Cost Model**
   - I/O cost
   - CPU cost
   - Cost formulas for operators

3. **Optimizer**
   - Rule-based optimization
   - Join order optimization
   - Access path selection

### What You'll Learn
- Cardinality estimation
- Selectivity calculation
- Cost-based optimization
- Query rewriting rules
- Join enumeration algorithms

### Estimated Time: 3-4 weeks

---

## Phase 8: Advanced Features (Pick & Choose)

### Possible Extensions
- [ ] Parallel query execution
- [ ] Adaptive indexing
- [ ] Advanced compression (LZ4, bit packing)
- [ ] Vectorized execution (SIMD)
- [ ] Materialized views
- [ ] Query compilation
- [ ] Distributed execution
- [ ] Analytics-specific optimizations

---

## Current Focus: Phase 1 -> Phase 2 Transition

### Status
- Phase 1 (Storage Foundation) mostly complete
- One bug to fix: BufferPool LRU eviction
- Ready to move to Phase 2 (WAL/Recovery) after cleanup

### Immediate Next Steps (Following Mandatory Workflow)

#### Task 1: Fix BufferPool Eviction Bug

**Phase 0: Already done** (we understand the bug from previous work)

**Phase 1: Implement fix**
- Review existing eviction code
- Explain the bug: why evicted pages are empty
- Apply fix: ensure dirty pages flush before eviction
- Document what we learned

**Phase 2: Verify**
- Run SimpleLRUEviction test (should fail first)
- Apply fix
- Run test again (should pass)
- Reflect on what the bug taught us

#### Task 2: Design WAL (Write-Ahead Logging)

**Phase 0: Clarify & Plan** (START HERE)
- Review existing doc/design/wal/log_manager.md (if exists)
- Discuss: What is WAL? Why do databases need it?
- Explore alternatives (ARIES, PostgreSQL approach, simple logging)
- Decide: Which approach for learning?
- Create design document BEFORE coding

**Phase 1: Implement** (AFTER design approved)
- Build log record format
- Build log manager
- Integrate with buffer pool
- Explain concepts as we go

**Phase 2: Verify**
- Write crash recovery test
- Verify data survives simulated crash
- Reflect on what we learned about durability

---

## Development Workflow (MANDATORY)

**NEVER jump straight to code.** Follow these phases in order:

### Phase 0: Clarify & Plan (REQUIRED)

**Goal**: Understand what we're building and why

1. **Check for existing design**
   - Look in `doc/design/{module}/`
   - If found: Ask "Should I use this design?"
   - If not found: Ask "Have you designed this yet?"

2. **Understand the scope**
   - What are we building/learning?
   - Is this exploratory or building on prior work?
   - Requirements and constraints?
   - Start simple or full implementation?

3. **Suggest learning-appropriate scope**
   - Flag if too complex for initial learning
   - "Start with X to learn the concept, add Y later?"

4. **Create plan**
   - Break into learning-focused steps
   - One concept at a time

**PAUSE here** - Get user confirmation before proceeding

### Phase 1: Implement & Teach Loop

**For each element you're implementing:**

1. **Identify what you need**: What pattern/concept applies here?

2. **Discover existing patterns**:
   - Use code-search MCP to find similar implementations
   - Launch Idioms Agent for detailed patterns if needed
   - Already know the pattern? Apply directly

3. **Explain as you code**:
   - Brief comment on what you're implementing
   - Note the database/C++ concept it demonstrates
   - Call out tricky parts
   - Flag learning moments: "This is how database X handles Y"

4. **Write the code**:
   - Follow discovered patterns
   - Keep it readable
   - Add comments for non-obvious choices
   - Verbose is OK if it helps learning

5. **Next element**: Move to the next piece

**Balance**: Code quality yes, but understanding over perfection

### Phase 2: Verify & Reflect (MANDATORY)

**Test what you built:**
1. Build the code
2. Run relevant tests  
3. Report results

**When things break**:
- Errors teach you about the system!
- Explain what the error means
- Reproduce, fix, verify
- Note what you learned from debugging

**After verification:**
- Reflect: "What did we learn from building this?"
- Note surprises or "aha!" moments
- Suggest optional extensions

**Update documentation:**
- If you learned something surprising -> update design docs
- Note gotchas for future reference

### Documentation Structure

**Design Documents** (`doc/design/{module}/`):
- Problem -> Approach -> Alternatives -> Decision rationale
- Created BEFORE implementing

**Learning Notes** (`doc/learnings/`):
- `doc/learnings/cpp/`: C++ concepts, patterns, gotchas
- `doc/learnings/database/`: Database theory, algorithms, papers
- Created WHEN we learn something worth remembering
- Format: Explanation -> Example -> Why it matters

**When to Pause**:
- After Phase 0 planning (confirm scope)
- After test/build failures (explain, then ask next steps)
- Before adding significant complexity
- After successful implementation (reflect before moving on)

---

## Progress Tracking

### How to Know You're Ready to Move On:

**Phase 1 Complete When:**
- All storage tests pass
- WAL basic implementation working
- Can survive simple crash (recovery test)
- Understand: page lifecycle, caching, durability

**Phase 2 Complete When:**
- Recovery tests pass
- Understand: WAL protocol, checkpointing
- Can explain ARIES algorithm

**Phase 3 Complete When:**
- Can store/retrieve columnar data
- Basic compression working
- Understand: column storage benefits/costs

*(Continue for each phase)*

---

## Learning Resources

### Books
- "Database Internals" by Alex Petrov
- "Designing Data-Intensive Applications" by Martin Kleppmann
- "Transaction Processing" by Gray & Reuter

### Papers
- ARIES recovery algorithm
- C-Store (columnar storage)
- MonetDB (column-at-a-time execution)

### Code References
- PostgreSQL (production reference)
- BusTub (CMU 15-445 - educational)
- DuckDB (modern analytics DB)

### Your Own Resources
- doc/design/ -> Design documents
- doc/design_exploration.md -> Brainstorming sessions
- tests/ -> Working examples

---

## Time Estimates

**Total estimated time**: 6-8 months (working part-time)

- Phase 1: 3-4 weeks (mostly done)
- Phase 2: 2-3 weeks
- Phase 3: 3-4 weeks
- Phase 4: 2-3 weeks
- Phase 5: 3-4 weeks
- Phase 6: 2-3 weeks
- Phase 7: 3-4 weeks

**Note**: These are learning estimates, not production development. Take time to understand, not just implement.

---

## Success Metrics

**You'll know this project succeeded when:**
- You can explain how a database stores data on disk
- You understand why WAL is necessary
- You can debug buffer pool issues
- You know why column stores are fast for analytics
- You can explain MVCC to someone else
- You've built something that actually works!

**Not measured by:**
- Lines of code
- Performance benchmarks
- Feature completeness
- Comparison to production databases

This is a learning journey, not a product launch.
