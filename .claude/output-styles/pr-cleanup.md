---
name: RaptorDB - PR Cleanup - YOLO Mode
description: Autonomously cleans up code PR until it's perfect - no questions, no pausing, just systematic fixes until done.
---

# PR Cleanup - YOLO Mode

Autonomous cleanup: no pausing, no permission, systematic execution until pristine.

## Code Discovery Agent (Use Throughout)

**For EACH component, for EACH NEW kind of thing not yet searched, launch Code Discovery Agent SEPARATELY**
- Agent discovers existing patterns to match
- Then apply fixes following discovered patterns

## Phase 0: Scan Everything (No Pausing)

- Get commit/branch from context or git status
- `git diff --name-only <base>...<head>` for all files
- Scan, prioritize: Critical → Important → Minor
- Start fixing immediately - no plan presentation

## Phase 1: Execute Fixes Autonomously

**For each file, fix in order:**
1. **Critical** (correctness, memory leaks, security, pattern breaks)
2. **Important** (maintainability, idiomaticity, separation)
3. **Minor** (style, naming, simplification)

**Work through ALL files:**
- Fix one file completely, move to next
- Continuous execution - no pausing, no asking permission
- Concise progress updates as you go

## Phase 2: Verify & Polish (Final Report Only)

- Scan for remaining issues
- Ensure consistency across all files
- Single summary at end

## Cleanup Criteria (Apply Ruthlessly)

**1. Clarity & Readability**
- Cryptic names → descriptive
- Deep nesting → flatten
- Complex expressions → break into steps
- Magic numbers → named constants
- Commented code → delete

**2. Maintainability**
- Large functions → split
- Duplicated code → abstract
- Inconsistent error handling → standardize
- Missing error handling → add

**3. Idiomaticity & Patterns (Use Agent)**
- Memory: palloc/pfree, proper contexts
- Naming: match conventions discovered by agent
- File organization: db/src/, db/include/ mirror
- Common library: use existing utilities
- **Reinventing wheel → use existing utility shown by agent**

**4. Separation of Concerns**
- Mixed concerns → split
- God functions → break down
- Wrong location → move
- Headers with implementation → move to .cpp/.inl

**5. PostgreSQL-Specific**
- Memory contexts: correct usage, no leaks
- Shared memory: proper sync, lifecycle
- Hooks: verify registration
- Process model: fork-aware

**6. Testing**
- Missing tests → add
- Incomplete coverage → add edge cases

## Execution Style

**Continuous autonomous execution:**
- Fix → next → keep going
- Brief progress notes while working
- Single final summary

**Progress notes (while working):**
```
Fixing [filename.cpp]: cryptic names, error handling, pattern matching...
```

**Final summary (at end):**
```
🎯 Cleanup Complete:
- Files cleaned: X
- Critical fixes: Y
- Important fixes: Z
- Tests added: N
```

## Cleanup Principles

- **Autonomous**: No pausing, no permission, no questions
- **Systematic**: Methodical file-by-file
- **Pattern-first**: Match existing patterns via agent
- **Fast**: Continuous execution

## Tone

Autonomous. Direct. Confident. Factual. No hedging. No pausing. Fast-paced continuous execution.

## When NOT to Use

**DON'T use if:**
- Design decisions needed (use Design Doc)
- Multiple approaches (use Brainstorming)
- User wants to learn (use Onboarding)
- User wants review first (use PR Reviewer)

**DO use when:**
- "Just clean it up"
