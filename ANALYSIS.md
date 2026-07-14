# Cloud Data Reconciliation Engine — Architecture Analysis & Improvement Roadmap

> Assessment of the current implementation against proposed CDC-reconciliation
> enhancements. Clearly separates **what is already covered** from **genuine gaps**.

---

## Executive Summary / Core Strategy

The **Cloud Data Reconciliation Engine** is a solid foundation. Based on the actual
code (`CompareEngine.java`, `OracleQueryStrategy`, `SnowflakeQueryStrategy`,
`DiffRepository`, `README.md`), it already implements an **enterprise-grade
streaming sort-merge zipper** with **DB-side hash pushdown** and a **CDC buffer
window**. Several proposed "improvements" are **already implemented** — below is a
precise breakdown of what is covered, partially covered, and genuinely missing.

---

## What Is Already Covered (Do Not Re-Build)

### ✅ Pushdown Computation — DONE
`OracleQueryStrategy` / `SnowflakeQueryStrategy` build dialect-specific SQL that
computes MD5 hashes and composite-key concatenation in the database. The engine
only pulls back `pkComposite` + `rowHash` strings (`Row.java`), never full rows.
Confirmed in `CompareEngine.run()` which compares `srcRow.rowHash()` vs
`destRow.rowHash()`.

### ✅ Streaming / No Full Load Into Memory — DONE
`OracleBatchIterator` + `SnowflakeRowIterator` use `setFetchSize(50000)` and
`autoCommit=false` (Oracle cursor streaming). Comparison is **O(n) lock-step**;
neither table is fully materialized.

### ✅ CDC False-Positive Mitigation (Buffer Window) — PARTIALLY DONE
The `bufferMinutes` high-water-mark filter (`WHERE modified_col < NOW() - 15 min`)
prevents in-flight rows from being compared at all — a simpler, effective
alternative to a 30-second "Pending Verification" retry loop.

### ✅ Changed-Columns Detection — DONE (backend)
`detectChangedColumns()` performs a targeted, per-PK, column-level diff **only for
HASH_MISMATCH rows**, using `ValueNormalizer` to avoid type/format false positives.
The `CHANGED_COLUMNS` value is already computed and stored.

### ✅ Multi-Schema / Batch Scale — DONE
`reconciler.mappings` + `/api/start-batch` with `schemaIds` and
`dest-filter-column/value` already handle many-Oracle-schemas-into-shared-Snowflake-tables.

---

## What Is Genuinely Missing or Weak (Real Opportunities)

### ❌ True Temporal Snapshot Alignment (Snowflake Time Travel)
The buffer window reduces lag noise but does **not** guarantee identical temporal
snapshots. Highest-value upgrade: use Oracle `AS OF` / a cutoff timestamp **and**
Snowflake `AT(TIMESTAMP => ...)` so both sides read the exact same logical instant.
Not present in `SnowflakeQueryStrategy` today.

### ❌ Tiered Reconciliation (Count → Bucket Checksum → Row Deep-Dive)
Every run is a full **Level 3** row-by-row zipper. No cheap Level 1
(count + max(modified)) or Level 2 (bucketed hash) pre-pass.

### ❌ Schema Drift Detection
`SchemaInspector` exists and `ColumnInfo` is loaded on both sides, but the engine
does **not** proactively compare Oracle vs Snowflake column sets/types and warn
before diffing. Easy win — mostly wiring existing code.

### ⚠️ PK Range Chunking / Parallel Workers
Comparison is single-threaded lock-step. Streams fine, but no parallelism across
PK ranges. Optional; only matters at extreme scale.

### ❌ Remediation Script Generation ("Self-Healing")
No generation of corrective `UPDATE`/`INSERT` SQL from diff rows. `pkComposite`,
diff type, and changed columns are already stored, so this is a natural addition.

### ❌ Alerting / Webhooks (Slack, PagerDuty, Email)
No threshold-based alerting on diff count.

### ⚠️ UI Enhancements (mostly frontend)
Colored pills for changed columns, side-by-side Git-style diff modal, time-series
filters, and log quick-filters are **UI-only** — the backend data
(`CHANGED_COLUMNS`, both hashes, diff types) already exists to power all of them.

---

## Edge Case & Mitigation Matrix

| # | Failure Mode | Current State | Mitigation |
|---|---|---|---|
| 1 | CDC replication lag → false positives | Buffer window only | Add Time Travel snapshot alignment for exactness |
| 2 | Schema drift (ALTER TABLE) | `ColumnInfo` loaded but not compared | Pre-flight `SchemaInspector` diff + block/warn |
| 3 | Type/format mismatch (NUMBER vs VARCHAR, timestamp precision) | Handled via `ValueNormalizer` | Extend normalizer coverage; unit-test edge types |
| 4 | Huge table cost/time | Full Level-3 every run | Add Level 1/2 tiered pre-pass + PK chunking |
| 5 | NULL in composite PK (`split("~", -1)`) | Fragile — `~` collision risk | Use length-prefixed or delimiter-safe PK encoding |

---

## Concrete Next Steps / Implementation Roadmap

1. **Time Travel snapshot support** — add optional `asOfTimestamp` to `JobConfig`;
   inject `AT(TIMESTAMP => ...)` in `SnowflakeQueryStrategy` and `AS OF TIMESTAMP`
   (or SCN) in `OracleQueryStrategy`.
2. **Schema-drift pre-flight** — compare `srcCols` vs `dstCols` in
   `CompareEngine.run()` before the zipper; surface a warning in `JobStatus`.
3. **Tiered mode** — add `reconLevel` (1/2/3); implement count/max-modified and
   bucketed-hash queries.
4. **Remediation generator** — new endpoint that turns a stored `DiffRecord` into
   `UPDATE`/`INSERT` SQL.
5. **UI polish** — changed-column pills + side-by-side diff modal (backend already
   provides the data).
6. **Alerting** — webhook config + post-job threshold check.

---

## Bottom Line

Roughly **half** the proposed suggestions are already built (pushdown hashing,
streaming, changed-columns, multi-schema, CDC buffer). The *net-new, high-value*
work is **Time Travel snapshot alignment**, **schema-drift detection**, **tiered
reconciliation**, and **remediation scripts**.

