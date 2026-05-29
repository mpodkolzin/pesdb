# Write-Path Roadmap — Wiring the WAL to Durable Page Storage

**Status:** 🧭 Roadmap / brainstorm outcome (2026-05-28). No code yet.
**Builds on:** `doc/design/wal/lsn.md` (LSN substrate, ✅ done),
`doc/design/storage/file-layout.md` (columnar target design),
`doc/design/storage/buffer_pool_manager.md`, `page.md`.

This doc records the architectural decision for how PesDB connects its two
currently-disconnected halves, and the slice sequence to get there. It is the
parent for the slice-level design docs that follow.

---

## The problem: two disconnected universes

The codebase has two halves that have never met:

- **Universe A — WAL/engine.** `Database` → `LogManager` → `RecoveryManager`.
  Storage is an in-memory `std::map`. Data is **row-oriented, schemaless
  `vector<int64_t>` tuples**. `INSERT_TUPLE` logs a whole row; `CREATE_TABLE`
  isn't logged at all.
- **Universe B — storage.** `DiskManager` → `BufferPoolManager` → `Page`, plus
  `Schema` and the `file-layout.md` design. **Columnar and typed**: Page 0 is a
  catalog, each *column* is a linked list of `ColumnDataPage`s (fixed-width
  int64 arrays), VARCHAR via dictionary encoding. It persists to disk but has
  **no WAL and no writer driving it**.

"Wiring the write path" = marrying them.

### The hard seam

The WAL speaks **rows**; the storage design speaks **columns**. Applying one
insert to columnar storage fans out into N page mutations (one per column) plus
catalog updates — i.e. multi-page atomicity, which is real database hard mode
(full-page writes, per-page LSN tracking). We want to avoid paying that on the
live write path while we're learning the recovery mechanics.

---

## Decision: delta store + tuple mover (don't choose row vs columnar — sequence it)

Adopt the **delta-store + tuple-mover** pattern (the canonical modern HTAP
columnstore design; matches the user's sister team's Postgres columnar flow):

```
                    ┌──────────────────────────────────────────────┐
   INSERT row  ───► │ 1. WAL append (row redo record) → returns LSN │
                    │ 2. fetch/alloc HEAP PAGE via buffer pool       │
                    │ 3. write row into page slot, page_lsn = LSN    │
                    │ 4. unpin dirty                                 │
                    └──────────────────────────────────────────────┘
                                      │ (settled data)
                                      ▼  async, later
                    ┌──────────────────────────────────────────────┐
   Tuple Mover ───► │ scan heap rows → build ColumnDataPage chains   │
                    │ → flip catalog pointer → reclaim heap pages    │
                    └──────────────────────────────────────────────┘

  Page 0 = CATALOG: schema per table + heap-chain head (+ later: columnar heads)
  Recovery: load pages, redo WAL records where lsn > page_lsn, rebuild read cache
```

- **Heap pages = delta/write store.** Cheap row appends, **one insert = one page
  mutation** → simple recovery / `page_lsn` / write-ahead.
- **Columnar `ColumnDataPage` chains = read store**, produced by an async
  **Tuple Mover** from settled (immutable) heap batches — where columnar
  conversion is naturally clean.

**Prior art:** Vertica (WOS→ROS via Tuple Mover), SQL Server clustered
columnstore (delta rowgroups → compressed columnar rowgroups),
Druid/Snowflake/Databricks (row ingest → columnar segments).

**Why it fits the learning goals:** keeps the WAL row-oriented (no impedance
mismatch), keeps recovery on easy mode, and gives all the columnar work
(`file-layout.md`) a proper home in the Tuple Mover. Nothing built is throwaway;
it's staged.

### Settled sub-decisions

- **Schema/catalog: in from the start.** `CREATE_TABLE` becomes a logged record;
  `Database` uses `Schema`; Page 0 holds the catalog.
- **In-memory `std::map`: kept as a throwaway read cache** (rebuilt from heap
  pages on open), replaced by the page/columnar scan path in a later session.
- **Heap pages are the durable source of truth on disk; WAL is redo for them.**

---

## Slice sequence

| Slice | Theme | Learning goal | New mechanics |
|---|---|---|---|
| **2a** | Heap write path + schema/catalog | (iii) catalog/persistence | `CREATE_TABLE` WAL record; `Schema` in `Database`; Page 0 catalog; fixed-width row heap page format; `Insert` → buffer pool → heap page; map rebuilt from pages on open |
| **2b** | Crash recovery onto pages | (i) recovery mechanics — *the payoff* | `page_lsn` consumed: redo only records with `lsn > page_lsn`; write-ahead invariant (`flushed_lsn_` + `FlushUpTo` in `LogManager`; buffer pool flushes WAL before evicting a dirty page) |
| **3** | Tuple Mover → columnar | (ii) columnar internals | `ColumnDataPage` chains, background heap→columnar conversion, columnar scan path |
| **4+** | Dictionary VARCHAR, vectorized scan, checkpoints | mix | builds on the above |

Note: `page_lsn` on `Page` (originally lsn.md "slice 2 of 3") now lands in **2b**,
where it gains a real consumer — not added blindly in isolation.

---

## Next concrete step: design doc for **Slice 2a**

Scope for slice 2a (no crash recovery — that's 2b):

1. `CREATE_TABLE` log record; carry a `Schema` (typed columns) instead of bare
   table names.
2. Page 0 catalog: serialize `Schema` (already supported) + each table's
   heap-chain head page id; bootstrap on open.
3. Heap page format: simple fixed layout `[next_page_id][row_count][rows...]`,
   each row = `num_columns × int64`. (Row-major sibling of `ColumnDataPage`.)
4. `Insert` write path: append WAL → fetch-or-allocate the table's last heap
   page via `BufferPoolManager` → append row → `SetPageLSN(lsn)` → unpin dirty.
   (This is also where `page_lsn` is first added to `Page`, now with a writer.)
5. `Scan`/open: rebuild the in-memory map by scanning heap pages through the
   buffer pool.

End state of 2a: **data survives a clean restart via real disk pages**,
schema-typed, catalog-bootstrapped. Crash-during-write recovery is 2b.

### Open questions to resolve in the 2a design doc

- 2a/2b split as above, or fold crash recovery into the first slice?
- Heap layout: fixed-width int64-only rows to start (defer VARCHAR/dictionary
  to the columnar slice), or handle STRING columns in the heap store too?
