# Database Internals Concepts

This directory contains notes on database theory, algorithms, and implementation patterns learned while building PesDB.

## What Goes Here

- **Storage concepts**: Page layouts, buffer pools, file formats
- **Query processing**: Query planning, execution, optimization
- **Transactions**: MVCC, WAL, recovery, isolation levels
- **Indexing**: B-trees, hash indexes, columnar indexes
- **Algorithms**: Join algorithms, sorting, aggregation
- **Papers**: Summaries of important database research

## Format

Each file should cover one concept:
- **Theory** - What is this concept?
- **Real-world examples** - How do PostgreSQL, SQLite, etc. do it?
- **Tradeoffs** - Different approaches and their implications
- **Our approach** - Why we chose our implementation
- **References** - Papers, blog posts, source code

## Example

```markdown
# Buffer Pool Eviction Policies

## Theory
Buffer pool needs to decide which pages to evict when full.

## Real-World Examples
- PostgreSQL: Clock sweep (approximation of LRU)
- MySQL InnoDB: LRU with young/old sublists
- SQLite: Simple LRU

## Tradeoffs
- LRU: Simple but can't handle sequential scans well
- Clock: Better scan resistance, slightly more complex
- 2Q/ARC: Best performance, most complex

## Our Approach
[What we implemented and why]

## References
- [Paper link]
- [Source code link]
```
