# C++ Concepts and Patterns

This directory contains notes on C++ concepts, patterns, and idioms encountered while building PesDB.

## What Goes Here

- **Language features**: RAII, move semantics, templates, smart pointers, etc.
- **Patterns**: Common C++ idioms and best practices
- **Gotchas**: Tricky behavior, common mistakes, undefined behavior
- **Modern C++**: C++11/14/17/20 features we use

## Format

Each file should cover one concept:
- **What it is** - Clear explanation of the concept
- **Why it exists** - The problem it solves
- **How to use it** - Code examples in context
- **Watch out for** - Common mistakes or gotchas

## Example

```markdown
# Move Semantics

## What
Move semantics allow transferring resources from one object to another without copying.

## Why
Avoids expensive copies when an object is about to be destroyed anyway.

## How
[code examples]

## Watch Out For
- Moved-from objects are in valid but unspecified state
- Can't move const objects
```
