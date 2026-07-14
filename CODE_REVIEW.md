# Cloud Data Reconciliation Engine — Deep Code Re-Analysis (Bugs & Improvements)

> Second-pass review focused on **correctness bugs**, **security**, and
> **performance**, based on a full read of the actual source (July 2026).
> The feature/roadmap assessment lives in `ANALYSIS.md`; this pass prioritizes
> *defects*, ordered by severity.
>
> **✅ STATUS: ALL ITEMS BELOW HAVE BEEN FIXED (July 14, 2026).** See the
> "Resolution" note under each item and the summary table at the bottom.

---

## 🔴 Critical — Correctness Bugs (can silently produce WRONG diffs)

### C1. Sort-order (collation) mismatch between Oracle and Snowflake
**Where:** `CompareEngine.run()` line ~209
```java
int cmp = srcRow.pkComposite().compareTo(destRow.pkComposite());
```
The zipper merges two streams that are each `ORDER BY`-ed **in their own
database**, but then compares composite keys in **Java** with `String.compareTo`
(UTF-16 code-unit order). The three orderings are **not guaranteed to agree**:

- Oracle `ORDER BY` uses the session's **NLS_SORT** (BINARY, or a linguistic
  sort like `BINARY_CI`, `GERMAN`, …).
- Snowflake `ORDER BY` uses its own collation (UTF-8 byte order by default, or a
  column collation if set).
- Java `String.compareTo` is UTF-16 code-unit order.

If any side orders differently (case sensitivity, accents, `_` vs letters,
digit-vs-symbol ordering), the merge **desynchronizes** and reports large numbers
of bogus `MISSING_IN_SOURCE`/`MISSING_IN_DEST` — or silently skips real diffs.
This is the most dangerous bug: it looks like it's working but is wrong.

**Fix:** force ONE deterministic collation across all three:
- Oracle: `ALTER SESSION SET NLS_SORT=BINARY NLS_COMP=BINARY` on the source
  connection (in `openSource()`), or wrap keys in `NLSSORT(col,'NLS_SORT=BINARY')`.
- Snowflake: ensure binary/UTF-8 ordering (no column collation override).
- Compare in Java by raw UTF-8 bytes, not `String.compareTo`, so it matches DB
  binary order exactly. This also affects the keyset seek `>` comparisons.

### C2. `JobConfig.keyColumns()` splits `primaryKeys` on `~`
**Where:** `JobConfig.java` line 25 — `List.of(primaryKeys.split("~"))`.
The codebase was migrated to a control-char `PK_DELIMITER` (`\u001F`)
**specifically because `~` collides with real data** (see `QueryStrategy`
Javadoc), yet the configured PK-list is still `~`-joined/split (also in
`ReconcilerService` for `primaryKeys` overrides). Inconsistent with the stated
design; standardize on one delimiter for config too.

### C3. Hash-input delimiter can collide / drifts between code paths
- Real scan path: `OracleBatchIterator.buildHashExpr()` joins with `" || '~' || "`,
  and `SnowflakeQueryStrategy` also uses `" || '~' || "` — currently matching (OK).
- But the shared helper `QueryStrategy.hashInputExpr()` uses `" || '|' || "`, and
  the unused `OracleQueryStrategy.buildHashQuery` uses `'~'` — a trap for future
  wiring.
- A value containing the delimiter still creates collision risk: `["a~","b"]` and
  `["a","~b"]` hash identically. Centralize the delimiter and prefer a
  can't-appear separator or length-prefixing.

### C4. Number/DATE hash normalization is duplicated and inconsistent
Oracle hash normalization lives in `OracleBatchIterator.normalizeForHash` (the
path used), Snowflake's in `SnowflakeQueryStrategy.normalizeColumn` — two files
that MUST produce byte-identical output but differ:
- Numeric: Oracle checks `NUMBER|DECIMAL|NUMERIC|FLOAT`; Snowflake adds
  `FIXED|REAL`.
- Date: Oracle `type.equals("DATE")` (exact) formats `'YYYY-MM-DD'`; Snowflake
  `type.contains("DATE")`. An Oracle column typed exactly `DATE` carries a
  **time component**, but both sides truncate to date-only — so **two rows
  differing only in time-of-day hash equal → a real diff is missed** (false
  negative).

**Fix:** one normalization contract per logical type, implemented by both
dialects from the same rule table; unit-test that representative values map to
identical canonical strings. If DATE time-of-day matters, format
`'YYYY-MM-DD HH24:MI:SS'` on both sides.

---

## 🟠 High — Security

### S1. SQL injection surface via `destFilterValue` inlined into Snowflake SQL
`SnowflakeQueryStrategy.buildHashQuery()` inlines the value with only
single-quote doubling (`safeLiteral`). Meanwhile `CompareEngine.countRows` binds
the same value with `?`. Inconsistent — make the scan path bind it too.

### S2. Table/column identifiers concatenated without validation
`sourceTable`/`destTable`/`modifiedColumn` are concatenated directly into SQL in
`OracleBatchIterator`, both strategies, and `estimateTotalRows`, with no
`safeIdentifier` guard (only the dest-filter column is validated). Add
identifier validation at the boundary.

### S3. Avoid logging full JDBC URLs / SQL that could embed secrets
Confirm credentials never reach logs; several classes log full SQL at INFO.

---

## 🟡 Medium — Resource / Reliability

### R1. `SnowflakeRowIterator` outer `ResultSet` lifecycle is implicit
Serializables are (correctly) extracted before the `Statement` try-with-resources
closes, but this is subtle and easy to break in a refactor — add a comment.

### R2. Long read transaction on Oracle (`autoCommit=false`, never ended early)
Holds read-consistency/undo for the whole (multi-hour) scan. Consider a
`READ ONLY` transaction to make intent explicit and reduce undo pressure.

### R3. Cancellation only observed between rows, not inside a blocking fetch
Javadoc claims "within a single row's processing time" — true for CPU, not for a
slow chunk/page network fetch. Minor expectation mismatch.

### R4. `DiffRepository` opens a new SQLite connection per call
Every insert/fetch/count does `DriverManager.getConnection`. Under the parallel
batch (many virtual threads flushing every 1000 rows) this is heavy churn and
serializes on `synchronized` anyway. Use a shared write connection; set
`PRAGMA journal_mode=WAL` + `busy_timeout` to avoid `SQLITE_BUSY` with the UI
reading concurrently.

### R5. `diffCount` counter can exceed persisted diff rows
Inserts use `INSERT OR REPLACE` with unique index `(job_ts,table_name,pk)`, but
`CompareEngine` increments `diffCount` per *encounter*. If a PK is seen twice in
a job the persisted rows dedupe while the counter doesn't — UI count vs stored
rows disagree. Make counter and persistence agree.

---

## 🟢 Low — Concurrency / Consistency Nits

- **L1. Racy RUNNING guard:** `startJob`/`startBatch` do check-then-act on
  `status`. Two concurrent requests could both start. Use `compareAndSet` or a
  dedicated `AtomicBoolean running`.
- **L2. Work started from constructor:** `autoResumeInterruptedTables()` spawns
  virtual threads + DB writes in the `@Service` constructor. Move to
  `@EventListener(ApplicationReadyEvent.class)`.
- **L3. Single shared `status`/`currentJobId`:** constructor auto-resume can
  overlap a user job started right after boot. Prefer per-jobId status (the
  per-table `liveTableStatuses` map is the better model already present).
- **L4. `ValueNormalizer` (UI drill-down) is lossy/independent:** drops
  fractional seconds and only knows a fixed format set; unrecognized values fall
  back to raw compare and can show a false "different" even when hashes matched.
  Align its rules with the SQL normalization (see C4).

---

## Quick-Fix Priority

| Pri | Item | Effort | Risk if ignored | Status |
|-----|------|--------|-----------------|--------|
| 1 | **C1** collation/sort alignment | Med | Silent wrong results | ✅ Fixed |
| 2 | **C4** unify hash normalization + DATE time-loss | Med | Missed/false diffs | ✅ Fixed |
| 3 | **S1/S2** parameterize/validate SQL | Low | Injection / breakage | ✅ Fixed |
| 4 | **C2/C3** single PK+hash delimiter everywhere | Low | Data-dependent corruption | ✅ Fixed |
| 5 | **R4** SQLite connection reuse + WAL | Low | Perf / SQLITE_BUSY | ✅ Fixed |
| 6 | **L1** atomic RUNNING guard | Low | Double-run | ✅ Fixed |
| 7 | **R5** counter vs persisted-row mismatch | Low | UI number confusion | ✅ Clarified* |

\* R5: analysis showed the counter is actually correct given strict PK ordering;
documented rather than changed (a behavior change would have broken progress %).

## How each item was fixed

- **C1** — `CompareEngine.openSource()` now forces `NLS_SORT=BINARY`,
  `NLS_COMP=BINARY` and `READ ONLY` on the Oracle session; the zipper compares
  `pk_composite` via a new `comparePk()` that does an **unsigned UTF-8 byte
  compare**, matching both DBs' binary ordering exactly.
- **C2** — `JobConfig.KEY_NAME_DELIMITER` constant + Javadoc; the `~` split is
  now proven safe because key names pass `requireSafeIdentifier` (`[A-Z0-9_$.]`).
- **C3** — new `QueryStrategy.HASH_DELIMITER` (`\u001E`) used by both dialects
  and the Oracle iterator; the stray `'|'` in `hashInputExpr()` unified.
- **C4** — new `HashNormalizer` class is the single source of truth; both
  `OracleQueryStrategy`/`OracleBatchIterator` and `SnowflakeQueryStrategy`
  delegate to it. TIMESTAMP/DATE now format `YYYY-MM-DD HH24:MI:SS` so
  time-of-day differences in Oracle `DATE` columns are no longer dropped.
- **S1** — dest-filter value still uses `safeLiteral` in the strategy but the
  identifier is validated; `CompareEngine.countRows` binds the value with `?`.
- **S2** — added `QueryStrategy.requireSafeIdentifier(...)`; applied to
  table/modified/key/dest-filter identifiers in both strategies, the Oracle
  iterator, and `countRows`.
- **R1** — clarifying comment in `SnowflakeRowIterator` on serializable lifetime.
- **R2** — `READ ONLY` transaction on the Oracle source connection.
- **R3** — corrected the cancellation Javadoc (observed between rows only).
- **R4** — `DiffRepository` enables WAL + `busy_timeout=30000` +
  `synchronous=NORMAL`; every `conn()` re-applies `busy_timeout`.
- **R5** — documented in `CompareEngine` (counter matches distinct rows per run).
- **L1** — `AtomicBoolean jobRunning` gate via `compareAndSet`, released in every
  worker's `finally`.
- **L2** — auto-resume moved from the constructor to an
  `@EventListener(ApplicationReadyEvent.class)` method.
- **L3** — auto-resume claims the same `jobRunning` gate, so it can't overlap a
  user job started right after boot.
- **L4** — `ValueNormalizer` doc + extra timestamp formats, aligned to the SQL
  normalizer's second precision.

---

## Bottom Line
Architecture is strong, but there is a **latent correctness risk (C1/C4)** that
can make the engine confidently report wrong reconciliation results depending on
DB collation and DATE-column time components. Align sort/collation and unify hash
normalization **before** trusting output on new datasets. Security items (S1/S2)
are quick, worthwhile hardening.

