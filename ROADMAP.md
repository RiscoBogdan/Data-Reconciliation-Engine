# Cloud Data Reconciliation Engine — Implementation Roadmap

> Phased, sprint-by-sprint plan to evolve the streaming Oracle → Snowflake
> sort-merge reconciliation engine per `ANALYSIS.md` gaps and the mandatory
> standards in `instruction.md`. Sprints are 2 weeks.

---

## 0. Current-State Baseline (Important Correction)

Before planning net-new work, the plan is anchored to what the repo **actually**
contains today (verified against source):

- **Framework is already Spring Boot 3.3 on Java 21**, not Javalin.
  `Main.java` is `@SpringBootApplication` + `@ConfigurationPropertiesScan`;
  `ReconcilerController` is `@RestController`; `ReconcilerService` is `@Service`;
  `DiffRepository` is `@Repository`. **No Javalin migration is required** — the
  "Javalin → Spring Boot" concern is already resolved. Remaining framework work
  is *consistency hardening* (DI for engine/strategies, bean-managed connection
  pooling), covered in Phase 0.
- **No test source tree exists** (`src/test` absent, no JUnit/Testcontainers
  dependency in `pom.xml`). A test harness is a **hard prerequisite** for the
  "tests required for all non-trivial changes" rule and is delivered first.
- Secrets today are read via `props.oracle().password()` /
  `props.snowflake().password()` from `AppProperties`; confirm these bind from
  env vars (`application.yml.example`) and are never logged. `JobConfig.toString()`
  and `log.info("...request: {}", config)` must be audited for leakage.
- Known fragilities already flagged: PK split on `"~"` in `JobConfig.keyColumns()`,
  `CompareEngine.detectChangedColumns()`, and `ReconcilerService.diffDetails()`;
  single-threaded lock-step zipper in `CompareEngine.run()`; per-call
  `DriverManager.getConnection` (no pooling).

---

## 1. Overview & Sequencing Rationale

Work is ordered so that each phase unblocks the next and de-risks the highest-value,
lowest-dependency items first:

1. **Phase 0 — Foundations** (test harness, connection pooling, DI cleanup,
   secret/logging audit, PK-encoding safety). Everything else depends on a test
   harness and safe PK handling.
2. **Phase 1 — Correctness & Trust** (schema-drift pre-flight, Time Travel
   snapshot alignment, `ValueNormalizer` edge coverage). These reduce
   false positives and make results trustworthy before we scale or automate.
3. **Phase 2 — Efficiency at Scale** (tiered reconciliation L1/L2/L3, PK-range
   chunking / parallel workers). Depends on stable correctness so cheap pre-passes
   don't mask real drift.
4. **Phase 3 — Actionability** (remediation script generation, alerting/webhooks).
   Depends on trustworthy diffs (Phase 1) so generated SQL and alerts are correct.
5. **Phase 4 — UX & Observability** (UI polish, metrics, dashboards). Consumes all
   backend data produced earlier; safe to do last.

Dependency chain (must-precede):
`Test harness → PK-safe encoding → Schema drift → Time Travel → Tiered modes →
Chunking → Remediation → Alerting → UI/Observability`.

---

## 2. Cross-Cutting Concerns

### 2.1 Framework decision (Javalin → Spring Boot)
Already Spring Boot. Policy going forward: **all new endpoints** via
`@RestController`, **all new logic** via `@Service`, **all persistence** via
`@Repository`, **all config** via `@ConfigurationProperties`/`@Configuration`.
`CompareEngine` and the `QueryStrategy` implementations are currently `new`-ed
manually; Phase 0 introduces a `@Configuration`-provided `CompareEngineFactory`
and Spring-managed `QueryStrategy` beans (Strategy pattern retained) so the engine
is testable via DI.

### 2.2 Security (traceable to `instruction.md` §5)
- **No secrets in code or logs**: audit `JobConfig.toString()`, all `log.*(config)`
  calls, and any query logging (`log.info("Source query:\n{}", srcSql)` in
  `CompareEngine`) — ensure no credentials/connection strings appear. Add a
  `SecureLogging` guideline + unit test asserting redaction.
- **Parameterized queries only**: dynamic identifiers (table/column names) can't be
  bound as parameters, so continue the `safeIdentifier()` allow-list pattern
  (`[A-Z0-9_\$]+`) everywhere identifiers are interpolated; centralize it into one
  `SqlIdentifiers` helper and unit-test injection attempts. New webhook/remediation
  features use prepared statements or the allow-list.
- **Fail closed**: schema-drift, alert-delivery, and remediation generation must
  abort (or refuse to emit SQL) on validation/security errors rather than continue.
- **Least privilege & secret sources**: webhook tokens (Slack/PagerDuty/SMTP) load
  from env/secret manager via `AppProperties`, never hardcoded, never logged.

### 2.3 Testing strategy
- **Unit**: JUnit 5 + AssertJ + Mockito for query builders (`OracleQueryStrategy`,
  `SnowflakeQueryStrategy`), `ValueNormalizer`, PK codec, schema-drift comparator,
  remediation SQL generator, alert threshold logic.
- **Integration**: Testcontainers (Oracle XE) for Oracle-side SQL + streaming;
  SQLite is embedded so `DiffRepository` is testable directly. Snowflake has no
  container — abstract its SQL behind the existing `QueryStrategy` interface and
  assert generated SQL strings; use a stubbed JDBC `Connection`/`ResultSet` (or a
  local H2/DuckDB dialect shim) for `SnowflakeRowIterator` iteration logic.
- **Contract**: golden-file tests on generated SQL per dialect to catch regressions.
- **Coverage gate**: JaCoCo, fail build under target on changed packages.

### 2.4 Backward compatibility
- All new `JobConfig`/`BatchRunRequest` fields are **optional** (nullable records)
  with safe defaults so existing API clients and `application.yml` keep working.
- PK-encoding change (Phase 0) is the one breaking-risk item: version the encoding
  and keep `"~"` legacy decode path behind a config flag; migrate stored diffs or
  namespace by `job_ts`.
- SQLite schema changes remain additive via the existing idempotent `ALTER TABLE`
  migration pattern in `DiffRepository.initialise()`.

### 2.5 Observability
- Introduce Micrometer (Spring Boot Actuator) for per-job metrics (rows scanned,
  diff counts by type, phase durations, tier used). Structured logs with job/table
  context (never secrets). `/actuator/health` + `/actuator/metrics` exposed.

### 2.6 Documentation
- Update `README.md`, `application.yml.example`, and `ANALYSIS.md` whenever config,
  API, or behavior changes (gate in `instruction.md` §6). Each feature ships an
  ADR under `docs/adr/`.

---

## 3. Phase 0 — Foundations & Hardening

### Sprint 0.1 — Test Harness & CI Gate
- **Goal**: Establish the testing baseline mandated by `instruction.md` §2/§6.
- **Create**: `src/test/java/com/reconciler/...` tree; `TestFixtures` builders for
  `JobConfig`/`BatchRunRequest`; JaCoCo + Surefire config in `pom.xml`;
  CI pipeline stage.
- **Modify**: `pom.xml` — add `spring-boot-starter-test`, `org.testcontainers:oracle-xe`,
  `mockito-core`, `assertj-core` (test scope).
- **Tasks**: unit tests for existing `OracleQueryStrategy.buildHashQuery`,
  `SnowflakeQueryStrategy.buildHashQuery`, `ValueNormalizer.normalize`,
  `JobConfig.keyColumns/effectiveBufferMinutes`; golden SQL snapshots; wire JaCoCo
  threshold (start low, ratchet up).
- **API/config schema**: none.
- **Security**: add a test asserting `JobConfig.toString()` and controller request
  logging never emit credentials/connection strings.
- **Docs**: `README.md` "Running tests" section.
- **DoD**: `mvn test` green in CI; coverage report published; secret-redaction test
  passing.
- **Effort**: M (1 sprint). **Risks**: Oracle Testcontainer image size/licensing →
  mitigate with slim XE image and `@Tag("integration")` opt-in profile.

### Sprint 0.2 — Delimiter-Safe PK Encoding & Connection Pooling
- **Goal**: Remove `"~"` collision risk and per-call `DriverManager` connections.
- **Create**: `engine/PkCodec.java` (length-prefixed or escaped encoding) with
  `encode(List<String>)`/`decode(String)`; `config/DataSourceConfig.java`
  (`@Configuration`) providing HikariCP `DataSource` beans for Oracle/Snowflake/SQLite.
- **Modify**: `JobConfig.keyColumns()` and all `pkComposite.split("~", -1)` sites
  (`CompareEngine.detectChangedColumns`, `ReconcilerService.diffDetails`) to use
  `PkCodec`; `OracleQueryStrategy`/`SnowflakeQueryStrategy` composite-key expr to
  emit the same encoding; replace `openSource()/openDest()` and
  `DriverManager.getConnection` calls in `CompareEngine`/`ReconcilerService`/
  `ReconcilerController` with injected pooled `DataSource`s.
- **API/config**: add `reconciler.pk-encoding` flag (`LEGACY_TILDE` | `LENGTH_PREFIXED`,
  default `LEGACY_TILDE` for compat); Hikari pool sizes in `application.yml`.
- **Security**: pooled datasources centralize credential handling; no secret logging.
- **Test**: unit tests for NULL-in-PK, embedded `~`, empty segments, multi-byte;
  round-trip property tests; integration test that both dialects sort identically
  under new encoding (critical for sort-merge correctness).
- **Docs**: ADR on PK encoding + migration note; `application.yml.example` updates.
- **DoD**: zero raw `split("~")`; both DB sides order-consistent; pools active;
  legacy path still passes.
- **Effort**: L. **Risks**: sort-order change breaks the zipper → gate behind flag,
  add ordering integration test, keep default legacy until Phase 1 opt-in.

---

## 4. Phase 1 — Correctness & Trust

### Sprint 1.1 — Schema-Drift Pre-Flight Detection
- **Goal**: Compare Oracle vs Snowflake column sets/types **before** the zipper and
  surface a warning (fail-closed option) — wiring existing `SchemaInspector`.
- **Create**: `engine/SchemaDriftChecker.java` (pure comparator returning a
  `SchemaDriftReport` record: missing-in-source, missing-in-dest, type-mismatch);
  extend `model/JobStatus` (or add `warnings` field) to carry drift warnings.
- **Modify**: `CompareEngine.run()` to call the checker after
  `loadColumnInfo(srcConn,...)` / `loadColumnInfo(dstConn,...)` and populate
  `statusRef`; `ReconcilerController.results()` to expose `warnings`;
  `ReconcilerService.startJob/startBatch` to propagate.
- **API/config**: `JobConfig.schemaDriftPolicy` (`WARN` default | `BLOCK`);
  `results` response gains `warnings[]`.
- **Security**: identifier allow-list on any reported names; fail-closed when
  policy=`BLOCK`.
- **Test**: unit tests for added/dropped/retyped columns; integration test that a
  drifted table yields a warning and (under BLOCK) aborts before diffing.
- **Docs**: README "Schema drift" section; ADR.
- **DoD**: drift visible in UI status payload; BLOCK aborts cleanly; tests green.
- **Effort**: M. **Risks**: type-name normalization across dialects (NUMBER vs
  FIXED) → reuse `ValueNormalizer` type-family mapping; treat as warning not error.

### Sprint 1.2 — Temporal Snapshot Alignment (Time Travel)
- **Goal**: Read both sides at the same logical instant via Oracle `AS OF` and
  Snowflake `AT(TIMESTAMP => ...)`.
- **Create**: `engine/TemporalClause.java` helper building dialect-specific
  as-of clauses from a validated timestamp/SCN.
- **Modify**: `JobConfig` — add `asOfTimestamp` (ISO-8601) and optional `asOfScn`
  (Oracle); `OracleQueryStrategy.buildHashQuery` to inject
  `... FROM <table> AS OF TIMESTAMP TO_TIMESTAMP_TZ(?)` / `AS OF SCN ?`;
  `SnowflakeQueryStrategy.buildHashQuery` to inject `FROM <table> AT(TIMESTAMP => ?)`;
  ensure `OracleBatchIterator`/`SnowflakeRowIterator` bind the parameter (prepared
  statement) rather than string-concat.
- **API/config**: `JobConfig.asOfTimestamp`, `asOfScn` (both optional, null = current).
- **Security**: timestamp/SCN validated & bound as parameters (no concatenation);
  reject malformed input fail-fast.
- **Test**: unit golden-SQL per dialect with/without as-of; integration (Oracle
  Testcontainer) verifying `AS OF TIMESTAMP` returns historical snapshot; Snowflake
  SQL-string assertion + parameter-binding test.
- **Docs**: README "Time Travel snapshot alignment"; note retention limits.
- **DoD**: same-instant reads demonstrably reduce lag false positives; tests green.
- **Effort**: L. **Risks**: Snowflake/Oracle retention window exceeded → validate
  against retention, clear error message, fail-fast.

### Sprint 1.3 — ValueNormalizer Edge-Type Coverage
- **Goal**: Close type/format false positives (timestamp precision, numeric scale,
  boolean, NULL sentinels) in `ValueNormalizer` and the dialect `normalizeColumn`.
- **Modify**: `engine/ValueNormalizer.java` (add cases: timestamp-with-tz, high-scale
  numbers, boolean 0/1/true/false, CLOB/whitespace, unicode); align Oracle/Snowflake
  `normalizeColumn(ColumnInfo)` sentinel handling (`NULL_STR`/`NULL_NUM`/`NULL_DATE`).
- **API/config**: none.
- **Security**: none new.
- **Test**: exhaustive parameterized unit tests per type family; property-based tests
  for numeric trailing-zero and date-truncation symmetry Oracle↔Snowflake.
- **Docs**: inline Javadoc + normalization matrix in README.
- **DoD**: documented edge types covered by tests; measurable false-positive drop.
- **Effort**: M. **Risks**: over-normalization hides real diffs → keep hashing raw,
  normalize only in `detectChangedColumns`/`diffDetails` display path.

---

## 5. Phase 2 — Efficiency at Scale

### Sprint 2.1 — Tiered Reconciliation: Level 1 & Level 2
- **Goal**: Add cheap pre-passes — L1 count + `max(modifiedColumn)`, L2 bucketed
  checksum — before falling to full L3 zipper.
- **Create**: `engine/TieredReconciler.java` orchestrating levels; `engine/
  BucketChecksumStrategy` methods on `QueryStrategy` (L2 groups by
  `MOD(hash(pk), N)` and aggregates row hashes per bucket, compared bucket-by-bucket).
- **Modify**: `QueryStrategy` interface — add `buildCountQuery`, `buildBucketChecksumQuery`;
  implement in both dialect strategies; `CompareEngine.run()` to consult
  `TieredReconciler` and only deep-dive mismatched buckets (drives L3 chunk selection).
- **API/config**: `JobConfig.reconLevel` (1|2|3, default 3 for compat) and
  `bucketCount`.
- **Security**: bucket expressions use bound params / allow-listed identifiers.
- **Test**: unit golden SQL for count/bucket queries per dialect; integration proving
  identical tables short-circuit at L1, single-bucket drift isolates to L2→L3.
- **Docs**: README "Tiered reconciliation"; decision table for level selection.
- **DoD**: L1/L2 short-circuit verified; L3 scoped to drifted buckets; tests green.
- **Effort**: L. **Risks**: bucket hash function parity across dialects → assert with
  cross-DB integration fixture; fall back to full L3 on parity failure (fail-safe).

### Sprint 2.2 — PK-Range Chunking & Parallel Workers
- **Goal**: Parallelize the single-threaded lock-step comparison across PK ranges.
- **Create**: `engine/PkRangePlanner.java` (derive N ranges via NTILE/percentiles or
  bucket ids from 2.1); `engine/ChunkedCompareEngine` (or extend `CompareEngine`)
  running one zipper per range on the existing virtual-thread executor
  (`Executors.newVirtualThreadPerTaskExecutor` already used in `startBatch`).
- **Modify**: `CompareEngine.run()` to accept a PK-range predicate; strategies to
  add bounded `WHERE pk BETWEEN ? AND ?` (bound params); `ReconcilerService` to
  aggregate per-chunk `JobStatus` into totals; `DiffRepository.insertBatch` already
  thread-safe (`synchronized`) — validate contention.
- **API/config**: `JobConfig.parallelism` / `chunkCount` (default 1 = current behavior).
- **Security**: range bounds bound as params; per-connection from pool (Phase 0).
- **Test**: unit for range planning edge cases (skew, single row); integration that
  chunked run == single-threaded run diff set (determinism); SQLite writer-contention
  soak test.
- **Docs**: README performance/tuning; ADR on chunking strategy.
- **DoD**: N-worker run yields identical diffs to serial run; measurable speedup;
  tests green.
- **Effort**: XL. **Risks**: SQLite single-writer bottleneck / range skew → batch
  flush tuning, consider WAL mode, skew-aware planner; keep default serial.

---

## 6. Phase 3 — Actionability

### Sprint 3.1 — Remediation Script Generation
- **Goal**: Turn a stored `DiffRepository.DiffRecord` into corrective
  `UPDATE`/`INSERT`/`DELETE` SQL (targeting Snowflake by default).
- **Create**: `service/RemediationService.java` (`@Service`) +
  `engine/RemediationSqlBuilder.java` mapping diff types → SQL
  (`MISSING_IN_DEST`→INSERT, `MISSING_IN_SOURCE`→DELETE/flag, `HASH_MISMATCH`→UPDATE
  of `changed_columns`); new `@RestController` endpoint
  `GET /api/remediation?table=&pk=` (and bulk `POST /api/remediation`).
- **Modify**: `ReconcilerController` add mapping; reuse `diffDetails()` value fetch
  for current source values; `DiffRepository` add fetch-by-pk/table if needed.
- **API/config**: new endpoints; response is generated SQL **text only** (never
  auto-executed) plus a dry-run flag; `reconciler.remediation.enabled` guard.
- **Security (critical)**: **generate, never execute**; all identifiers via
  allow-list, all values via bound-parameter placeholders or safely escaped
  literals; require the value-source read to use parameterized queries; fail-closed
  if `changed_columns` can't be resolved; no PII in logs.
- **Test**: unit per diff-type → SQL golden files incl. NULLs and composite PKs;
  injection tests on identifiers/values; integration generating runnable SQL against
  Oracle Testcontainer (validate syntax only).
- **Docs**: README "Remediation scripts" incl. explicit "review before running"
  warning; ADR.
- **DoD**: correct, safe SQL for all three diff types; injection tests pass; no
  auto-execution path exists.
- **Effort**: L. **Risks**: unsafe generated SQL → mandatory dry-run, allow-list,
  reviewer sign-off; feature-flag default off.

### Sprint 3.2 — Alerting / Webhooks (Slack, PagerDuty, Email)
- **Goal**: Threshold-based notifications on diff counts after a job/batch.
- **Create**: `service/AlertService.java` (`@Service`) with `AlertChannel` Strategy
  impls (`SlackChannel`, `PagerDutyChannel`, `EmailChannel`); `config/AlertProperties`
  (`@ConfigurationProperties`); optional `@RestController` test endpoint
  `POST /api/alerts/test`.
- **Modify**: `ReconcilerService.startJob`/`startBatch` completion paths to invoke
  `AlertService` when `diffCount`/ratio exceeds threshold; add HTTP client
  (`RestClient`) bean.
- **API/config**: `reconciler.alerts.{enabled,threshold,ratioThreshold,channels[]}`;
  channel secrets (`slackWebhookUrl`, `pagerDutyRoutingKey`, SMTP creds) from
  **env/secret manager only**.
- **Security**: secrets never logged/echoed; outbound TLS; validate/escape message
  content (no raw PII, no connection strings); fail-closed but non-blocking (alert
  failure must not fail the job, logged with context).
- **Test**: unit threshold logic + message formatting; channel tests with mocked
  HTTP (WireMock); secret-redaction test.
- **Docs**: README "Alerting"; `application.yml.example` channel stubs; ADR.
- **DoD**: alert fires above threshold to configured channels; secrets externalized;
  tests green.
- **Effort**: M. **Risks**: alert storms / secret leakage → dedupe + rate-limit,
  redaction test, default disabled.

---

## 7. Phase 4 — UX & Observability

### Sprint 4.1 — UI Polish (Pills, Side-by-Side Diff, Trends, Log Filters)
- **Goal**: Frontend-only enhancements over existing backend data
  (`changed_columns`, both hashes, diff types, `/api/diff-details`).
- **Modify**: `src/main/resources/public/assets/js/app.js` (AG Grid cell renderers
  for colored changed-column pills, side-by-side git-style diff modal calling
  `/api/diff-details`, time-series trend filter using `created_at`, log
  quick-filters); `assets/css/app.css` (pill/diff styles); `index.html`/`flow.html`
  controls.
- **API/config**: optionally add `GET /api/trends` (diff counts over time from
  `DiffRepository`, grouped by `created_at`/`diff_type`).
- **Security**: escape all rendered DB values (XSS) in the diff modal/pills;
  no secrets in client payloads.
- **Test**: lightweight JS unit/DOM tests (or Playwright smoke) for renderers and
  modal; backend test for `/api/trends`.
- **Docs**: README UI section + screenshots.
- **DoD**: pills, diff modal, trend filter, log filters functional; XSS-safe.
- **Effort**: M. **Risks**: XSS via unescaped values → central escaping helper +
  test.

### Sprint 4.2 — Observability & Metrics
- **Goal**: Production-grade metrics/health per `instruction.md` §3 observability.
- **Create**: Micrometer meters in `CompareEngine`/`ReconcilerService`
  (rows scanned, diffs by type, phase/tier durations, chunk counts).
- **Modify**: `pom.xml` add `spring-boot-starter-actuator`; expose
  `/actuator/health`, `/actuator/metrics`, `/actuator/prometheus`; structured
  logging with job/table MDC context (no secrets).
- **API/config**: actuator exposure settings in `application.yml`.
- **Security**: restrict/secure actuator endpoints; ensure no sensitive tags/labels.
- **Test**: metric-registration unit tests; health endpoint integration test.
- **Docs**: README "Observability & metrics"; dashboard notes.
- **DoD**: key metrics visible; health green; endpoints secured; tests green.
- **Effort**: S–M. **Risks**: metric cardinality (per-table tags) → bounded tags,
  aggregate where needed.

---

## 8. Summary Timeline

| Phase | Sprint | Feature | Key Files | Effort |
|-------|--------|---------|-----------|--------|
| 0 Foundations | 0.1 | Test harness + CI/JaCoCo | `pom.xml`, `src/test/**` | M |
| 0 Foundations | 0.2 | PK-safe codec + connection pooling | `PkCodec`, `DataSourceConfig`, `JobConfig`, `CompareEngine`, strategies | L |
| 1 Correctness | 1.1 | Schema-drift pre-flight | `SchemaDriftChecker`, `CompareEngine`, `JobStatus`, controller | M |
| 1 Correctness | 1.2 | Time Travel snapshot alignment | `JobConfig`, `Oracle/SnowflakeQueryStrategy`, iterators | L |
| 1 Correctness | 1.3 | ValueNormalizer edge coverage | `ValueNormalizer`, dialect `normalizeColumn` | M |
| 2 Efficiency | 2.1 | Tiered L1/L2 reconciliation | `TieredReconciler`, `QueryStrategy`, `CompareEngine` | L |
| 2 Efficiency | 2.2 | PK-range chunking / parallel workers | `PkRangePlanner`, `CompareEngine`, `ReconcilerService` | XL |
| 3 Actionability | 3.1 | Remediation script generation | `RemediationService`, `RemediationSqlBuilder`, controller | L |
| 3 Actionability | 3.2 | Alerting / webhooks | `AlertService`, `AlertProperties`, channels | M |
| 4 UX/Obs | 4.1 | UI polish | `public/assets/js/app.js`, `app.css`, `/api/trends` | M |
| 4 UX/Obs | 4.2 | Observability & metrics | actuator, Micrometer, `pom.xml` | S–M |

Indicative duration: **~11 sprints (~22 weeks)** single-stream; Phases 3–4 can
partially parallelize once Phase 1 is complete.

---

## 9. Definition of Done (per `instruction.md` §6, applies to every sprint)
- Layered architecture preserved (`@RestController`/`@Service`/`@Repository`/`@Configuration`).
- Immutable records, small single-purpose methods, fail-fast validation, explicit
  exception handling.
- Parameterized queries / allow-listed identifiers; no hardcoded secrets; no
  PII/credential/connection-string logging; fail-closed on security errors.
- Unit + integration tests for all non-trivial changes; coverage gate met.
- `README.md` / `application.yml.example` / `ANALYSIS.md` / ADRs updated.
- No new high/critical vulnerabilities.

---

## 10. Open Decisions (need product/tech sign-off)
1. **PK-encoding default**: ship legacy `~` default in 0.2, flip to `LENGTH_PREFIXED`
   in Phase 1 after the ordering integration test proves the zipper invariant holds.
2. **Snowflake integration testing**: no container available — use golden-SQL +
   bound-parameter assertions plus a DuckDB/H2 dialect shim, unless a shared
   Snowflake test schema can be provided.
3. **Diff store at scale**: if chunking (2.2) is bottlenecked by SQLite's single
   writer, consider WAL mode first, then a possible swap to DuckDB/Postgres as an
   explicit follow-up sprint.

