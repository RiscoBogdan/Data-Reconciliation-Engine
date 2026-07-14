# Compare Engine — Master Analysis, Strategy & Implementation Roadmap

> Consolidated, production-grade document merging the full architecture analysis
> with the phased sprint roadmap. Supersedes `ANALYSIS.md` and the earlier
> `ROADMAP.md` by combining both into a single source of truth.
>
> **Stack:** Java 21 / Spring Boot 3.3 · Oracle (source) → Snowflake (dest) ·
> streaming sort-merge zipper · SQLite diff store · Bootstrap + AG Grid UI.

---

## Table of Contents
1. Current Architecture Assessment
2. Multi-Tiered Data Reconciliation Strategy
3. Performance & Scalability Architecture
4. Resilience & Fault Tolerance
5. Frontend UX Improvements
6. Edge Cases & Mitigation Matrix
7. Phased Implementation Roadmap (Sprint-by-Sprint)
8. Cross-Cutting Concerns
9. Summary Timeline
10. Definition of Done
11. Open Decisions

---

## 1. Current Architecture Assessment

### What the Engine Does Well
- **Sort-Merge Zipper (`CompareEngine.run()`)** — correct O(n) time, O(1) memory
  beyond the page buffer; fully streaming, no full-table JVM loads.
- **Keyset Pagination on Oracle (`OracleBatchIterator`)** — typed row-value
  constructor `(pk1, pk2) > (?, ?)` with `FETCH FIRST N ROWS ONLY`, two
  pre-compiled `PreparedStatement`s preserve the execution plan across pages,
  avoiding cursor timeouts on multi-hour scans.
- **Snowflake Arrow Chunk Streaming (`SnowflakeRowIterator`)** — uses
  `SnowflakeResultSetSerializable` to split a sorted result into ~50 MB Arrow
  chunks, keeping at most one decompressed chunk in memory.
- **CDC False-Positive Suppression (`sanitizeConfig`)** — removes `modifiedColumn`
  from the hash; `bufferMinutes` high-watermark filter bounds the comparison window.
- **Per-Table Parallelism (`ReconcilerService.startBatch`)** — `ExecutorCompletionService`
  over a virtual-thread executor (Java 21), up to 8 tables concurrently.
- **Column-Level Diff (`detectChangedColumns` / `fetchRowAsStrings`)** — re-fetches
  the single mismatched row on both sides; `ValueNormalizer` handles numeric
  trailing-zero and timestamp-precision divergence.
- **Sophisticated Frontend** — drag-and-drop PK/Hash zones, per-table config in
  `localStorage`, live Chart.js chart, AG Grid with diff-type badges, row-count caching.

### Architectural Gaps (with reasoning)
- **Gap 1 — No Job Resumability.** `CompareEngine.run()` holds one long-lived
  Oracle connection + one Snowflake execution. JVM crash at row 50M of 100M means
  restart from row 0; `OracleBatchIterator.lastPkObjects` is in-memory only.
- **Gap 2 — Single-Threaded Zipper.** No intra-table parallelism. `getChunks()` is
  exposed on `SnowflakeRowIterator` but never used for partition-split.
- **Gap 3 — SQLite Diff Store Contention.** `DiffRepository` is `synchronized` per
  method; 8 parallel table threads contend on one mutex (single writer).
- **Gap 4 — N+1 JDBC Diff Calls.** `detectChangedColumns` fires two point lookups
  per `HASH_MISMATCH` inside the tight loop — 200K round-trips for 100K mismatches.
- **Gap 5 — No Schema Drift Detection.** No pre-flight compatibility check; a
  column added to Oracle but not Snowflake makes every row `HASH_MISMATCH` silently.
- **Gap 6 — PK Sort-Order Assumption (CORRECTNESS BUG).** Zipper relies on
  `pkComposite` **lexicographic** compare. `CAST(NUMBER AS VARCHAR2)` makes
  `"9" > "10"` — false `MISSING_IN_*` pairs for numeric PKs wider than one digit
  unless zero-padded.
- **Gap 7 — No Connection Pooling / Retry.** Raw `DriverManager.getConnection`
  everywhere; no pool, no retry on ORA-12520 / Snowflake token expiry.
- **Gap 8 — No Rate Limiting on `/api/diff-details`.** Opens two DB connections
  synchronously on the HTTP thread; concurrent modal opens can exhaust Oracle conns.

---

## 2. Multi-Tiered Data Reconciliation Strategy

### Tier 1 — CDC Lag Buffer (implemented; harden to asymmetric)
Make the buffer window asymmetric — Oracle source lag ≠ Snowflake dest lag.
```java
public record JobConfig(
    // ...existing fields...
    Integer sourceBufferMinutes,  // NEW: Oracle-side exclusion window
    Integer destBufferMinutes,    // NEW: Snowflake-side exclusion window
    Integer bufferMinutes         // KEEP for backwards compat
) {
    public int effectiveSourceBufferMinutes() {
        return sourceBufferMinutes != null ? sourceBufferMinutes : effectiveBufferMinutes();
    }
    public int effectiveDestBufferMinutes() {
        return destBufferMinutes != null ? destBufferMinutes : effectiveBufferMinutes();
    }
}
```
`SnowflakeQueryStrategy.buildHashQuery()` applies its own:
`WHERE MODIFIED_AT < DATEADD('minute', -15, CURRENT_TIMESTAMP())`.

### Tier 2 — Schema Drift Pre-Flight Check
```java
public class SchemaCompatibilityChecker {
    public record CompatibilityReport(
        List<String> missingInDest, List<String> missingInSource,
        List<String> typeMismatches, boolean compatible) {}

    public static CompatibilityReport check(
            List<QueryStrategy.ColumnInfo> srcCols,
            List<QueryStrategy.ColumnInfo> dstCols,
            List<String> hashColumns) {
        Set<String> hashSet = new HashSet<>(hashColumns);
        // compare srcCols vs dstCols filtered to hashSet
    }
}
```
On `compatible == false`: **fail fast** (`SchemaIncompatibleException`) for critical
mismatches, or emit a warning and auto-exclude divergent columns (logging which).

### Tier 3 — PK Sort-Order Fix (CORRECTNESS CRITICAL)
Zero-pad numeric PKs in `OracleBatchIterator.buildPkExpr()` **and** `SnowflakeQueryStrategy`:
```java
private String normalizeForPkComposite(String column) {
    String type = typeByColumn.getOrDefault(column.toUpperCase(Locale.ROOT), "VARCHAR2");
    if (type.contains("NUMBER") || type.contains("INTEGER") || type.contains("INT")) {
        return "LPAD(CAST(" + column + " AS VARCHAR2(38)), 38, '0')"; // Oracle NUMBER max precision
    }
    return "CAST(" + column + " AS VARCHAR2(4000))";
}
```
Snowflake must match: `LPAD(CAST(pk_col AS VARCHAR(38)), 38, '0') || '~' || ...`.

### Tier 4 — Deferred Column-Level Diff (Eliminate N+1)
Record only mismatch PKs during the zipper; run a **bulk** post-pass in batches of 500:
```java
private void bulkDetectChangedColumns(Connection srcConn, Connection dstConn,
        List<String> mismatchedPks, List<ColumnInfo> srcCols, List<ColumnInfo> dstCols) {
    // Batches of 500 → WHERE pk_col IN (...) both sides
    // UPDATE diffs SET changed_columns = ? WHERE pk_composite = ? AND diff_type='HASH_MISMATCH'
}
```
Reduces N+1 to `ceil(mismatches/500)` queries — ~500× improvement.

---

## 3. Performance & Scalability Architecture

### Intra-Table Parallelism via PK Range Splitting
```java
public class RangePartitioner {
    // MIN/MAX pk → N equal-width numeric ranges, or NTILE() percentile ranges for strings
    public static List<PkRange> partition(Connection conn, String table,
            String pkCol, int n) throws SQLException { /* ... */ }
}
```
Each range gets its own `OracleBatchIterator` (`WHERE pk BETWEEN ? AND ?`) and
`SnowflakeRowIterator`; per-range zippers run on parallel virtual threads; results
merge into one diff stream. Peak heap = `N × BATCH_SIZE`.

### Connection Pooling via HikariCP
```java
@Configuration
public class DataSourceConfig {
    @Bean("oraclePool")
    public HikariDataSource oraclePool(AppProperties props) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(props.oracle().jdbcUrl());
        cfg.setUsername(props.oracle().user());
        cfg.setPassword(props.oracle().password());
        cfg.setMaximumPoolSize(20);
        cfg.setConnectionTimeout(30_000);
        cfg.setConnectionTestQuery("SELECT 1 FROM dual");
        cfg.setAutoCommit(false);
        return new HikariDataSource(cfg);
    }
    @Bean("snowflakePool")
    public HikariDataSource snowflakePool(AppProperties props) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(props.snowflake().jdbcUrl());
        cfg.setUsername(props.snowflake().user());
        cfg.setPassword(props.snowflake().password());
        cfg.setMaximumPoolSize(10); // warehouse concurrency-limited
        cfg.setConnectionTimeout(60_000);
        return new HikariDataSource(cfg);
    }
}
```

### SQLite Write Throughput Fix
```java
// DiffRepository constructor
s.execute("PRAGMA journal_mode=WAL");
s.execute("PRAGMA synchronous=NORMAL");   // fsync only at checkpoint
s.execute("PRAGMA cache_size=-65536");    // 64 MB page cache
```
Move `insertBatch` to a single background `BlockingQueue<List<DiffRecord>>` writer
thread — converts 8 contending writers into 1 serial writer (~0 lock wait).

### Snowflake Query Optimization
```sql
ALTER SESSION SET QUERY_TAG = 'compare_engine_job_123';
ALTER SESSION SET USE_CACHED_RESULT = TRUE;
```
Recommend `CLUSTER BY pk`; set JDBC `CLIENT_PREFETCH_THREADS=4` for parallel chunk
download; use `RESULT_SCAN` to cache intermediate results.

---

## 4. Resilience & Fault Tolerance

### Job Checkpointing
```sql
CREATE TABLE IF NOT EXISTS job_checkpoints (
    job_id       TEXT PRIMARY KEY,
    table_name   TEXT NOT NULL,
    last_pk_json TEXT NOT NULL,   -- JSON array of PK values
    src_count    INTEGER, dest_count INTEGER, diff_count INTEGER,
    updated_at   TEXT DEFAULT (datetime('now'))
);
```
Persist `lastPkObjects` every 100 pages (~5M rows). On restart with
`resumeFromCheckpoint = true`, seed `OracleBatchIterator` from `last_pk_json`.
Snowflake re-executes the full (cached) query.

### Partial Failure Handling / Retry with Backoff
```java
private TableRunResult runWithRetry(JobConfig cfg, int maxRetries) {
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
        TableRunResult r = runSingleTable(cfg);
        if (r.success()) return r;
        if (attempt < maxRetries) {
            log.warn("Retrying table {} (attempt {}/{})", cfg.sourceTable(), attempt + 1, maxRetries);
            Thread.sleep(Duration.ofSeconds((long) Math.pow(2, attempt)));
        }
    }
    return runSingleTable(cfg);
}
```

### Idempotent Reruns
```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_diffs_idempotent
    ON diffs(job_ts, table_name, pk_composite);
```
Use `INSERT OR REPLACE` so a rerun overwrites only re-processed tables.

### Graceful Shutdown
```java
private volatile boolean shutdownRequested = false;
@PreDestroy
public void shutdown() {
    shutdownRequested = true;
    log.info("Shutdown requested — jobs will stop at next page boundary");
}
```
`OracleBatchIterator.fetchNextBatch()` checks `shutdownRequested` before each page.

---

## 5. Frontend UX Improvements
1. **Real-Time Progress / ETA** — using cached row counts:
   `percentComplete = sourceCount/estimatedTotal*100`; per-table progress bar.
2. **Stale-Poll Indicator** — `consecutiveFailures` counter; after 3 failures show
   "⚠ Connection lost — retrying" + exponential backoff (2s→4s→8s), reset on recovery.
3. **Side-by-Side / Character-Level Diff** — integrate `diff-match-patch.js` (~30KB)
   in `openDiffModal` to distinguish 1-second timestamp drift from real changes.
4. **Keyboard Nav & Bulk Ops** — AG Grid checkbox selection + "Re-inspect selected";
   `Enter` opens modal; **CSV export** via `exportDataAsCsv()`.
5. **Per-Table Config Validation** — ⚠ icon next to tables with empty `primaryKeys`.
6. **SSE Instead of Polling** — replace 2s poll with `SseEmitter`:
   ```java
   @GetMapping(value = "/api/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
   public SseEmitter jobEvents() { /* push status while RUNNING */ }
   ```

---

## 6. Edge Cases & Mitigation Matrix

| # | Failure Mode | Root Cause | Mitigation |
|---|---|---|---|
| 1 | **Numeric PK sort corruption** | `CAST(pk AS VARCHAR2)` → lexicographic `"9">"10"`; false `MISSING_IN_*` pairs | `LPAD(CAST(pk AS VARCHAR2(38)),38,'0')` both sides; unit test PKs `[1,9,10,11,100]` |
| 2 | **Snowflake token expiry mid-job** | OAuth/JWT expires ~1h; conn held full job | Key-pair auth (no expiry) or `TOKEN_LIFETIME=12h`; retry+reconnect in `SnowflakeRowIterator.advance()` from last chunk |
| 3 | **Oracle ORA-01555 snapshot too old** | Long read exceeds `UNDO_RETENTION` | `AS OF SCN` captured at job start on all keyset pages; needs FLASHBACK privilege |
| 4 | **Hash collision false negative** | MD5 128-bit, cryptographically broken | `STANDARD_HASH(expr,'SHA256')` / `SHA2_HEX` for compliance use cases |
| 5 | **Composite PK with NULL components** | `CAST(NULL AS VARCHAR2)` → NULL; concat/compare breaks | `NVL(CAST(...), '__NULL__')` sentinel both sides; delimiter-safe encoding |
| 6 | **Clock skew Oracle vs app** | `CURRENT_TIMESTAMP` on DB vs app clock offsets buffer | Read Oracle `CURRENT_TIMESTAMP` at start, compute skew, adjust window; warn if >60s |
| 7 | **SQLite WAL corruption on kill -9** | Mid-commit `insertBatch` interrupted | WAL self-recovers; run `PRAGMA integrity_check` on startup; `wal_autocheckpoint=1000` |

---

## 7. Phased Implementation Roadmap (Sprint-by-Sprint)

> 2-week sprints. **Sequencing:** Foundations → Correctness → Performance →
> Resilience → Actionability → UX/Observability. Each item unblocks the next.

### Phase 0 — Foundations & Hardening

#### Sprint 0.1 — Test Harness & CI Gate
- **Goal:** testing baseline (`instruction.md` §2/§6); no `src/test` exists today.
- **Create:** `src/test/java/com/reconciler/**`, `TestFixtures`; JaCoCo + Surefire.
- **Modify:** `pom.xml` — `spring-boot-starter-test`, `testcontainers:oracle-xe`,
  `mockito-core`, `assertj-core`.
- **Tasks:** unit tests for `OracleQueryStrategy`/`SnowflakeQueryStrategy.buildHashQuery`,
  `ValueNormalizer`, `JobConfig`; golden SQL snapshots.
- **Security:** test that `JobConfig.toString()` / request logging never emit secrets.
- **DoD:** `mvn test` green in CI; coverage published; secret-redaction test passes.
- **Effort:** M. **Risk:** Oracle container size → slim XE + `@Tag("integration")`.

#### Sprint 0.2 — Connection Pooling & DI Cleanup
- **Goal:** replace raw `DriverManager` with HikariCP; Spring-manage engine/strategies.
- **Create:** `config/DataSourceConfig` (Oracle/Snowflake/SQLite pools),
  `CompareEngineFactory`, `QueryStrategy` beans.
- **Modify:** `CompareEngine`, `ReconcilerService`, `ReconcilerController`,
  `SchemaInspector` to inject pooled `DataSource`s; add transient-failure retry.
- **Config:** Hikari pool sizes + timeouts in `application.yml`.
- **Test:** pool exhaustion, retry-on-transient (mock ORA-12520).
- **DoD:** zero raw `getConnection`; pools active; retry covered.
- **Effort:** M. **Risk:** Snowflake warehouse concurrency → cap pool at 10.

### Phase 1 — Correctness & Trust

#### Sprint 1.1 — PK Sort-Order & NULL-Safe Encoding (CRITICAL)
- **Goal:** fix numeric lexicographic sort bug + NULL/delimiter safety.
- **Create:** `engine/PkCodec` (LPAD zero-pad numerics, `NVL` NULL sentinel,
  escaped delimiter); config `reconciler.pk-encoding` (`LEGACY_TILDE` | `SORT_SAFE`).
- **Modify:** `OracleBatchIterator.buildPkExpr()`, `SnowflakeQueryStrategy`,
  `JobConfig.keyColumns()`, all `split("~")` sites (`CompareEngine.detectChangedColumns`,
  `ReconcilerService.diffDetails`).
- **Test:** PKs `[1,9,10,11,100]` order identically both sides; NULL-in-PK; embedded
  `~`; multi-byte; **round-trip** + **cross-dialect ordering** integration test.
- **DoD:** both DB sides order-consistent; legacy path behind flag still passes.
- **Effort:** L. **Risk:** order change breaks zipper → gate behind flag + ordering test.

#### Sprint 1.2 — Schema-Drift Pre-Flight Detection
- **Goal:** compare Oracle vs Snowflake column sets/types before the zipper.
- **Create:** `engine/SchemaCompatibilityChecker` → `CompatibilityReport` record;
  `JobConfig.schemaDriftPolicy` (`WARN` | `BLOCK` | `AUTO_EXCLUDE`).
- **Modify:** `CompareEngine.run()` after `loadColumnInfo(...)`; propagate warnings
  into `JobStatus` and `/api/results`.
- **Test:** added/dropped/retyped columns; BLOCK aborts before diffing.
- **DoD:** drift visible in status; BLOCK aborts cleanly.
- **Effort:** M. **Risk:** cross-dialect type names (NUMBER vs FIXED) → type-family map.

#### Sprint 1.3 — Temporal Snapshot Alignment (Time Travel + SCN)
- **Goal:** same-instant reads via Oracle `AS OF SCN/TIMESTAMP` + Snowflake
  `AT(TIMESTAMP => ?)`; also mitigates ORA-01555.
- **Create:** `engine/TemporalClause`.
- **Modify:** `JobConfig.asOfTimestamp`/`asOfScn` (optional); inject bound-param
  clauses in both strategies + iterators.
- **Test:** golden SQL per dialect; Oracle Testcontainer historical read.
- **DoD:** same-instant reads reduce lag false positives.
- **Effort:** L. **Risk:** retention window exceeded → validate + fail-fast.

#### Sprint 1.4 — Asymmetric CDC Buffer & Clock-Skew Calibration
- **Goal:** `sourceBufferMinutes` / `destBufferMinutes`; skew-adjusted window.
- **Modify:** `JobConfig`, both strategies' buffer clauses; read Oracle
  `CURRENT_TIMESTAMP` at start, compute skew, warn if >60s.
- **Test:** buffer presence per side; skew offset math.
- **Effort:** M. **Risk:** over-exclusion → keep symmetric default.

### Phase 2 — Efficiency at Scale

#### Sprint 2.1 — Deferred Bulk Column-Diff (Eliminate N+1)
- **Goal:** move `detectChangedColumns` out of the hot loop into a post-pass.
- **Create:** `bulkDetectChangedColumns` (batches of 500, `WHERE pk IN (...)`).
- **Modify:** `CompareEngine.run()` to record mismatch PKs only; post-pass `UPDATE diffs`.
- **Test:** correctness parity vs per-row; batch boundary (499/500/501).
- **DoD:** ~500× fewer round-trips; identical `changed_columns`.
- **Effort:** L. **Risk:** IN-list limits → cap batch size per dialect.

#### Sprint 2.2 — SQLite WAL + Async Writer Thread
- **Goal:** remove 8-way writer contention.
- **Modify:** `DiffRepository` — WAL PRAGMAs; single `BlockingQueue` writer thread;
  `integrity_check` on startup; `wal_autocheckpoint=1000`.
- **Test:** concurrent enqueue soak; crash-recovery integrity.
- **Effort:** M. **Risk:** queue backpressure → bounded queue + flush tuning.

#### Sprint 2.3 — Tiered Reconciliation (L1 count / L2 bucket / L3 deep-dive)
- **Create:** `engine/TieredReconciler`; `QueryStrategy.buildCountQuery` /
  `buildBucketChecksumQuery` (`MOD(hash(pk), N)`).
- **Modify:** `CompareEngine.run()` deep-dives only mismatched buckets.
- **Config:** `JobConfig.reconLevel` (1|2|3, default 3), `bucketCount`.
- **Effort:** L. **Risk:** cross-dialect hash parity → fall back to L3.

#### Sprint 2.4 — Intra-Table PK-Range Parallelism
- **Create:** `engine/RangePartitioner` (MIN/MAX or NTILE), `ChunkedCompareEngine`.
- **Modify:** strategies add `WHERE pk BETWEEN ? AND ?`; aggregate per-chunk status.
- **Config:** `JobConfig.parallelism`/`chunkCount` (default 1).
- **Test:** chunked diff set == serial; skew planner; contention soak.
- **Effort:** XL. **Risk:** SQLite writer / skew → WAL writer (2.2) + skew-aware planner.

### Phase 3 — Resilience & Actionability

#### Sprint 3.1 — Job Checkpointing & Resumability
- **Create:** `job_checkpoints` table; checkpoint every 100 pages.
- **Modify:** `OracleBatchIterator` save/restore `lastPkObjects`; `JobConfig.resumeFromCheckpoint`.
- **Test:** simulated crash mid-scan resumes correctly.
- **Effort:** L. **Risk:** Snowflake can't resume mid-stream → re-execute (cached).

#### Sprint 3.2 — Retry, Idempotent Reruns & Graceful Shutdown
- **Modify:** `ReconcilerService.startBatch` `runWithRetry(maxRetries=2)`;
  `INSERT OR REPLACE` + `idx_diffs_idempotent`; `@PreDestroy` + `shutdownRequested`.
- **Test:** retry backoff; idempotent overwrite; shutdown at page boundary.
- **Effort:** M.

#### Sprint 3.3 — Remediation Script Generation
- **Create:** `service/RemediationService`, `engine/RemediationSqlBuilder`
  (`MISSING_IN_DEST`→INSERT, `MISSING_IN_SOURCE`→DELETE, `HASH_MISMATCH`→UPDATE);
  endpoints `GET/POST /api/remediation`.
- **Security (critical):** **generate, never execute**; allow-listed identifiers;
  bound-param / escaped values; dry-run; `reconciler.remediation.enabled` (default off).
- **Test:** golden SQL per diff-type incl. NULL/composite PK; injection tests.
- **Effort:** L.

#### Sprint 3.4 — Alerting / Webhooks (Slack, PagerDuty, Email)
- **Create:** `service/AlertService` + `AlertChannel` strategies; `AlertProperties`.
- **Modify:** batch completion invokes alerts above threshold; `RestClient` bean.
- **Security:** channel secrets from env/secret manager only; never logged;
  non-blocking; rate-limit/dedupe to avoid storms.
- **Test:** threshold logic; mocked HTTP (WireMock); secret-redaction.
- **Effort:** M.

### Phase 4 — UX & Observability

#### Sprint 4.1 — SSE, Progress/ETA & Stale-Poll Indicator
- **Create:** `GET /api/events` (`SseEmitter`).
- **Modify:** `app.js` → `EventSource`; per-table progress bars from cached counts;
  `consecutiveFailures` banner + backoff.
- **Effort:** M.

#### Sprint 4.2 — UI Polish (Pills, Char-Level Diff, Trends, Bulk Ops)
- **Modify:** `app.js`/`app.css`/`index.html` — changed-column pills, `diff-match-patch.js`
  char-level diff modal, `/api/trends` time-series, log quick-filters, checkbox
  bulk re-inspect, CSV export, config-validation ⚠ icons.
- **Security:** escape all rendered DB values (XSS).
- **Effort:** M.

#### Sprint 4.3 — Observability & Metrics
- **Modify:** `pom.xml` add `spring-boot-starter-actuator`; Micrometer meters
  (`@Timed` on `CompareEngine.run()`, gauges for active jobs, counters per diff type);
  expose `/actuator/{health,metrics,prometheus}`; MDC job/table context (no secrets).
- **Effort:** S–M. **Risk:** metric cardinality → bounded tags.

---

## 8. Cross-Cutting Concerns

### 8.1 Framework
Already Spring Boot 3.3 (no Javalin migration). New endpoints `@RestController`,
logic `@Service`, persistence `@Repository`, config `@ConfigurationProperties`.
DI-manage `CompareEngine` + `QueryStrategy` beans (Strategy pattern retained).

### 8.2 Security (`instruction.md` §5)
- No secrets in code/logs; audit `JobConfig.toString()`, `log.*(config)`, and query
  logging (`log.info("Source query:\n{}", srcSql)`); secret-redaction test.
- Parameterized queries only; identifiers via centralized `safeIdentifier()`
  allow-list (`[A-Z0-9_\$]+`); injection unit tests.
- Fail closed on schema-drift/alert/remediation validation errors.
- Secrets from env/secret manager (consider **Vault / AWS Secrets Manager** via
  Spring Cloud Vault), least privilege (Oracle `SELECT ANY TABLE` scrutiny).
- Consider **MD5 → SHA-256** for compliance-sensitive reconciliations.

### 8.3 Testing
JUnit 5 + AssertJ + Mockito (unit); Testcontainers Oracle XE + embedded SQLite
(integration); Snowflake via golden-SQL + bound-param assertions + H2/DuckDB shim;
golden-file SQL contract tests; JaCoCo coverage gate.

### 8.4 Backward Compatibility
All new `JobConfig`/`BatchRunRequest` fields optional with safe defaults. PK-encoding
versioned behind `reconciler.pk-encoding` flag. SQLite schema changes additive via
idempotent `ALTER TABLE` in `DiffRepository.initialise()`.

### 8.5 Cost Management
Snowflake `RESULT_SCAN` + `QUERY_HISTORY` to skip unchanged tables on frequent runs;
`QUERY_TAG` per job for attribution; `CLUSTER BY pk` guidance.

---

## 9. Summary Timeline

| Phase | Sprint | Feature | Key Files | Effort |
|---|---|---|---|---|
| 0 | 0.1 | Test harness + CI/JaCoCo | `pom.xml`, `src/test/**` | M |
| 0 | 0.2 | HikariCP pooling + DI + retry | `DataSourceConfig`, `CompareEngine`, `ReconcilerService` | M |
| 1 | 1.1 | **PK sort-order + NULL-safe encoding** | `PkCodec`, `OracleBatchIterator`, `SnowflakeQueryStrategy` | L |
| 1 | 1.2 | Schema-drift pre-flight | `SchemaCompatibilityChecker`, `CompareEngine`, `JobStatus` | M |
| 1 | 1.3 | Time Travel + SCN snapshot | `TemporalClause`, strategies, iterators | L |
| 1 | 1.4 | Asymmetric buffer + clock skew | `JobConfig`, strategies | M |
| 2 | 2.1 | Deferred bulk column-diff | `CompareEngine` | L |
| 2 | 2.2 | SQLite WAL + async writer | `DiffRepository` | M |
| 2 | 2.3 | Tiered L1/L2/L3 reconciliation | `TieredReconciler`, `QueryStrategy` | L |
| 2 | 2.4 | Intra-table PK-range parallelism | `RangePartitioner`, `ChunkedCompareEngine` | XL |
| 3 | 3.1 | Job checkpointing / resumability | `job_checkpoints`, `OracleBatchIterator` | L |
| 3 | 3.2 | Retry + idempotent + shutdown | `ReconcilerService`, `DiffRepository` | M |
| 3 | 3.3 | Remediation script generation | `RemediationService`, `RemediationSqlBuilder` | L |
| 3 | 3.4 | Alerting / webhooks | `AlertService`, `AlertProperties` | M |
| 4 | 4.1 | SSE + progress/ETA + stale-poll | `/api/events`, `app.js` | M |
| 4 | 4.2 | UI polish + char-diff + bulk ops | `app.js`, `app.css`, `/api/trends` | M |
| 4 | 4.3 | Observability & metrics | actuator, Micrometer | S–M |

Indicative duration: **~17 sprints (~34 weeks)** single-stream; Phases 3–4 can
partially parallelize once Phase 1 completes.

---

## 10. Definition of Done (per `instruction.md` §6, every sprint)
- Layered architecture (`@RestController`/`@Service`/`@Repository`/`@Configuration`).
- Immutable records, small methods, fail-fast validation, explicit exceptions.
- Parameterized queries / allow-listed identifiers; no hardcoded secrets; no
  PII/credential/connection-string logging; fail closed on security errors.
- Unit + integration tests for all non-trivial changes; coverage gate met.
- `README.md` / `application.yml.example` / this doc / ADRs updated.
- No new high/critical vulnerabilities.

---

## 11. Open Decisions (need sign-off)
1. **PK-encoding default flip** — ship `LEGACY_TILDE`, flip to `SORT_SAFE` after the
   cross-dialect ordering test proves the zipper invariant (Sprint 1.1). This is the
   highest-priority correctness item; consider hotfixing ahead of the full phase plan.
2. **Intra-table parallelism (2.4)** — only if single-table jobs exceed SLA; adds
   PK-range analysis + partial-result merge complexity.
3. **Snowflake integration testing** — no container; golden-SQL + bound-param
   assertions + DuckDB/H2 shim, unless a shared Snowflake test schema is provided.
4. **Diff store at scale** — if 2.4 is bottlenecked by SQLite's single writer even
   with WAL, evaluate DuckDB/Postgres swap as a follow-up sprint.
5. **Hash algorithm** — keep MD5 (speed) or move to SHA-256 (compliance)?
6. **Secret backend** — env vars vs Vault/AWS Secrets Manager for production.

