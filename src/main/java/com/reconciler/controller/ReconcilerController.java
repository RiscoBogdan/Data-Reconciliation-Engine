package com.reconciler.controller;

import com.reconciler.config.AppProperties;
import com.reconciler.config.BatchRunRequest;
import com.reconciler.config.JobConfig;
import com.reconciler.engine.SchemaInspector;
import com.reconciler.model.JobStatus;
import com.reconciler.repository.DiffRepository;
import com.reconciler.service.ReconcilerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.servlet.http.HttpServletResponse;
import com.reconciler.service.DiffExportService;

import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.LinkedHashMap;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.stream.Collectors;

/**
 * REST API for the Data Reconciliation Engine.
 *
 * <p>Endpoints:
 * <ul>
 *   <li>{@code POST  /api/start-job}  — launch a reconciliation job</li>
 *   <li>{@code GET   /api/results}    — paginated diff results + live status</li>
 *   <li>{@code DELETE /api/results}   — wipe results and reset status</li>
 *   <li>{@code GET   /api/schema}     — introspect Oracle + Snowflake table schemas</li>
 * </ul>
 */
@RestController
@RequestMapping("/api")
public class ReconcilerController {

    private static final Logger log = LoggerFactory.getLogger(ReconcilerController.class);

    private final ReconcilerService service;
    private final DiffRepository    diffRepo;
    private final AppProperties     props;
    private final DiffExportService exportService;

    public ReconcilerController(ReconcilerService service,
                                DiffRepository diffRepo,
                                AppProperties props,
                                DiffExportService exportService) {
        this.service  = service;
        this.diffRepo = diffRepo;
        this.props    = props;
        this.exportService = exportService;
    }

    // ── POST /api/start-job ──────────────────────────────────────────────

    @PostMapping("/start-job")
    public ResponseEntity<Map<String, Object>> startJob(@RequestBody JobConfig config) {
        log.info("Received start-job request: {}", config);
        long jobId = service.startJob(config);
        return ResponseEntity.accepted().body(Map.of("message", "Job started", "jobId", jobId));
    }

    // ── GET /api/results ─────────────────────────────────────────────────

    @GetMapping("/results")
    public Map<String, Object> results(
            @RequestParam(defaultValue = "1")   int    page,
            @RequestParam(defaultValue = "100") int    size,
            @RequestParam(required = false)     String search,
            @RequestParam(required = false)     String table,
            @RequestParam(required = false)     String fPk,
            @RequestParam(required = false)     String fDiffType,
            @RequestParam(required = false)     String fSourceHash,
            @RequestParam(required = false)     String fDestHash,
            @RequestParam(required = false)     String fChangedColumns,
            @RequestParam(required = false)     String fTable,
            @RequestParam(defaultValue = "false") boolean allHistory) {

        var jobRepo = service.jobRepository();
        JobStatus s = service.currentStatus();

        DiffRepository.Filters filters =
                new DiffRepository.Filters(fPk, fDiffType, fSourceHash, fDestHash, fChangedColumns, fTable);

        // Resolve scope: during RUNNING use the active job's timestamp;
        // otherwise use the latest job_ts that actually has diffs.
        String scopedJobTs;
        if (allHistory) {
            scopedJobTs = null;
        } else if ("RUNNING".equals(s.status())) {
            // Prefer the running job's timestamp so new diffs appear in real-time
            scopedJobTs = firstNonBlank(jobRepo.latestJobTs(), diffRepo.latestDiffJobTs());
        } else {
            scopedJobTs = firstNonBlank(diffRepo.latestDiffJobTs());
        }

        final List<Map<String, Object>> diffs;
        final int total;
        final List<String> tableOptions;
        final List<Map<String, Object>> baseSummaries;

        if (scopedJobTs != null && !scopedJobTs.isBlank()) {
            diffs = diffRepo.fetchByJobTs(scopedJobTs, page, size, search, table, filters);
            total = diffRepo.countByJobTs(scopedJobTs, search, table, filters);
            tableOptions = diffRepo.tableNamesByJobTs(scopedJobTs);
            baseSummaries = diffRepo.tableSummariesByJobTs(null, scopedJobTs);
        } else {
            diffs = diffRepo.fetch(page, size, search, table, filters);
            total = diffRepo.count(search, table, filters);
            tableOptions = diffRepo.tableNames();
            baseSummaries = diffRepo.tableSummaries(null);
        }

        // Row counts used to enrich each table's summary card.
        // - Live in-memory cache (service.tableRowCounts()) is fastest/most real-time for
        //   the CURRENT run, but it's cleared at the start of every new job/batch — so it
        //   only ever holds counts for the table(s) in the most recent run.
        // - jobRepo.latestRowCountsByTable() reads the persisted job_tables history, so it
        //   still has correct numbers for OTHER tables that aren't part of the active run.
        // - "All history" uses the same latest-per-table counts (not summed across every
        //   re-run) so it matches the Difference Log, which also dedupes to one row per
        //   table/PK rather than counting duplicates from repeated runs.
        Map<String, long[]> rowCounts = new LinkedHashMap<>(jobRepo.latestRowCountsByTable());
        if (!allHistory) {
            rowCounts.putAll(service.tableRowCounts()); // live values win for freshness
        }

        List<Map<String, Object>> summaries = baseSummaries.stream()
                .map(summary -> {
                    String tbl = String.valueOf(summary.get("tableName")).toUpperCase();
                    long[] counts = rowCounts.get(tbl);
                    Map<String, Object> enriched = new LinkedHashMap<>(summary);
                    enriched.put("sourceRowCount", counts != null ? counts[0] : 0L);
                    enriched.put("destRowCount",   counts != null ? counts[1] : 0L);
                    return enriched;
                })
                .collect(Collectors.toList());

        // "Total Rows Scanned" / Oracle / Snowflake row counts:
        // - When scoped to a single job (default view), use the live JobStatus counts
        //   for that run so RUNNING progress is reflected in real-time.
        // - When "all history" is requested, sum the latest-known count per DISTINCT
        //   table (not every historical run), so it doesn't double-count repeated runs
        //   of the same table — consistent with the deduped Difference Log.
        long outSourceCount;
        long outDestCount;
        if (allHistory) {
            outSourceCount = rowCounts.values().stream().mapToLong(c -> c[0]).sum();
            outDestCount   = rowCounts.values().stream().mapToLong(c -> c[1]).sum();
        } else {
            outSourceCount = s.sourceCount();
            outDestCount   = s.destCount();
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("status", s.status());
        out.put("sourceCount", outSourceCount);
        out.put("destCount", outDestCount);
        out.put("diffCount", s.diffCount());
        out.put("error", s.error() != null ? s.error() : "");
        out.put("totalRows", total);
        // Live "% complete" for the active scan (based on a best-effort COUNT(*)
        // estimate captured by CompareEngine) — -1/absent means "unknown" (e.g.
        // count query failed, or job isn't RUNNING), the UI should hide the bar then.
        out.put("progressPercent", s.percentComplete());
        out.put("page", page);
        out.put("pageSize", size);
        out.put("tableOptions", tableOptions);
        out.put("tableSummaries", summaries);
        out.put("rows", diffs);
        out.put("allHistory", allHistory);
        out.put("scopeJobTs", scopedJobTs == null ? "" : scopedJobTs);
        return out;
    }

    // ── DELETE /api/results ──────────────────────────────────────────────

    @DeleteMapping("/results")
    public Map<String, String> clearResults() {
        diffRepo.clear();
        service.jobRepository().clearAll();
        service.resetStatus();
        return Map.of("message", "Results cleared");
    }

    // ── GET /api/results/export ──────────────────────────────────────────

    /**
     * Streams the FULL filtered Difference Log (no pagination) as an Excel
     * (.xlsx) or XML file. All export logic (styling, charts, streaming) lives
     * in {@link DiffExportService}; the controller only resolves the scope and
     * delegates. Respects the same scope + search + per-column filters as the
     * on-screen table.
     */
    @GetMapping("/results/export")
    public void exportResults(
            @RequestParam(defaultValue = "xlsx") String format,
            @RequestParam(required = false)      String search,
            @RequestParam(required = false)      String table,
            @RequestParam(required = false)      String fPk,
            @RequestParam(required = false)      String fDiffType,
            @RequestParam(required = false)      String fSourceHash,
            @RequestParam(required = false)      String fDestHash,
            @RequestParam(required = false)      String fChangedColumns,
            @RequestParam(required = false)      String fTable,
            @RequestParam(defaultValue = "false") boolean allHistory,
            HttpServletResponse response) throws Exception {

        JobStatus s = service.currentStatus();
        String scopedJobTs;
        if (allHistory) {
            scopedJobTs = null;
        } else if ("RUNNING".equals(s.status())) {
            scopedJobTs = firstNonBlank(service.jobRepository().latestJobTs(), diffRepo.latestDiffJobTs());
        } else {
            scopedJobTs = firstNonBlank(diffRepo.latestDiffJobTs());
        }

        DiffRepository.Filters filters =
                new DiffRepository.Filters(fPk, fDiffType, fSourceHash, fDestHash, fChangedColumns, fTable);

        exportService.export(format, scopedJobTs, search, table, filters, response);
    }

    // ── GET /api/schema ──────────────────────────────────────────────────

    @GetMapping("/schema")
    public ResponseEntity<Map<String, Object>> schema(
            @RequestParam String source,
            @RequestParam String dest) {

        if (source.isBlank() || dest.isBlank()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "source and dest params required"));
        }
        try {
            var srcSchema  = SchemaInspector.fromOracle(source, props);
            var destSchema = SchemaInspector.fromSnowflake(dest, props);
            return ResponseEntity.ok(Map.of("source", srcSchema, "dest", destSchema));
        } catch (Exception e) {
            log.error("Schema inspection failed", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    // ── GET /api/tables ──────────────────────────────────────────────────

    @GetMapping("/tables")
    public ResponseEntity<Map<String, Object>> tables(
            @RequestParam String db,
            @RequestParam(required = false) String schema,
            @RequestParam(required = false) String database,
            @RequestParam(required = false, defaultValue = "") String q) {
        try {
            List<SchemaInspector.TableRef> rows;
            if ("oracle".equalsIgnoreCase(db)) {
                rows = SchemaInspector.listOracleTables(schema, q, props);
            } else if ("snowflake".equalsIgnoreCase(db)) {
                rows = SchemaInspector.listSnowflakeTables(database, schema, q, props);
            } else {
                return ResponseEntity.badRequest().body(Map.of("error", "db must be oracle or snowflake"));
            }
            return ResponseEntity.ok(Map.of("rows", rows));
        } catch (Exception e) {
            log.error("Table scan failed", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    // ── GET /api/schemas ─────────────────────────────────────────────────

    @GetMapping("/schemas")
    public ResponseEntity<Map<String, Object>> schemas(
            @RequestParam String db,
            @RequestParam(required = false) String database,
            @RequestParam(required = false, defaultValue = "") String q,
            @RequestParam(required = false, defaultValue = "false") boolean mappedOnly) {
        try {
            if ("oracle".equalsIgnoreCase(db)) {
                if (mappedOnly) {
                    String needle = q == null ? "" : q.trim().toUpperCase();
                    var rows = props.mappings().stream()
                            .filter(AppProperties.SchemaMapping::isUsable)
                            .map(AppProperties.SchemaMapping::oracleSchema)
                            .map(String::trim)
                            .map(String::toUpperCase)
                            .distinct()
                            .filter(s -> needle.isBlank() || s.contains(needle))
                            .map(SchemaInspector.SchemaRef::new)
                            .toList();
                    return ResponseEntity.ok(Map.of("rows", rows));
                }
                return ResponseEntity.ok(Map.of("rows", SchemaInspector.listOracleSchemas(q, props)));
            }
            if ("snowflake".equalsIgnoreCase(db)) {
                return ResponseEntity.ok(Map.of("rows", SchemaInspector.listSnowflakeSchemas(database, q, props)));
            }
            return ResponseEntity.badRequest().body(Map.of("error", "db must be oracle or snowflake"));
        } catch (Exception e) {
            log.error("Schema scan failed", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    // ── GET /api/schema-mappings ───────────────────────────────────────────

    @GetMapping("/schema-mappings")
    public ResponseEntity<Map<String, Object>> schemaMappings() {
        var rows = props.mappings().stream()
                .map(m -> Map.of(
                        "schemaId", m.schemaId(),
                        "oracleSchema", m.oracleSchema(),
                        "snowflakeDatabase", m.snowflakeDatabase(),
                        "snowflakeSchema", m.snowflakeSchema(),
                        "destFilterColumn", m.destFilterColumn(),
                        "destFilterValue", m.destFilterValue(),
                        "enabled", m.enabled(),
                        "usable", m.isUsable()
                ))
                .toList();
        return ResponseEntity.ok(Map.of("rows", rows));
    }

    // ── GET /api/oracle-databases ─────────────────────────────────────────

    @GetMapping("/oracle-databases")
    public ResponseEntity<Map<String, Object>> oracleDatabases() {
        String sql = "SELECT SYS_CONTEXT('USERENV','DB_NAME') AS DB_NAME, SYS_CONTEXT('USERENV','CON_NAME') AS CON_NAME FROM dual";
        try (Connection conn = DriverManager.getConnection(
                props.oracle().jdbcUrl(), props.oracle().user(), props.oracle().password());
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {

            List<Map<String, String>> rows = new java.util.ArrayList<>();
            while (rs.next()) {
                rows.add(Map.of(
                        "dbName", rs.getString("DB_NAME"),
                        "containerName", rs.getString("CON_NAME")
                ));
            }
            return ResponseEntity.ok(Map.of("rows", rows));
        } catch (Exception e) {
            log.error("Oracle database discovery failed", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    // ── GET /api/match-tables ────────────────────────────────────────────

    @GetMapping("/match-tables")
    public ResponseEntity<Map<String, Object>> matchTables(
            @RequestParam String sourceSchema,
            @RequestParam String destSchema,
            @RequestParam(required = false) String destDatabase,
            @RequestParam(required = false, defaultValue = "") String q) {
        try {
            var src = SchemaInspector.listOracleTables(sourceSchema, q, props);
            var dst = SchemaInspector.listSnowflakeTables(destDatabase, destSchema, q, props);

            Set<String> dstNames = dst.stream().map(t -> tailName(t.qualifiedName())).collect(Collectors.toSet());
            List<String> rows = src.stream()
                    .map(t -> tailName(t.qualifiedName()))
                    .filter(dstNames::contains)
                    .sorted()
                    .toList();
            return ResponseEntity.ok(Map.of("rows", rows));
        } catch (Exception e) {
            log.error("Table matching failed", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    // ── POST /api/start-batch ────────────────────────────────────────────

    @PostMapping("/start-batch")
    public ResponseEntity<Map<String, Object>> startBatch(@RequestBody BatchRunRequest req) {
        long jobId = service.startBatch(req);
        return ResponseEntity.accepted().body(Map.of("message", "Batch started", "jobId", jobId));
    }

    // ── GET /api/diff-details ─────────────────────────────────────────────

    @GetMapping("/diff-details")
    public ResponseEntity<Map<String, Object>> diffDetails(@RequestParam String pk,
                                                           @RequestParam(required = false) String table) {
        if (pk == null || pk.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "pk is required"));
        }
        try {
            var rows = service.diffDetails(pk, table);
            return ResponseEntity.ok(Map.of("pk", pk, "table", table == null ? "" : table, "rows", rows));
        } catch (Exception e) {
            log.error("Diff details failed for pk={}", pk, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    private String tailName(String qualified) {
        int dot = qualified.lastIndexOf('.');
        return dot >= 0 ? qualified.substring(dot + 1).toUpperCase() : qualified.toUpperCase();
    }

    private String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) return v;
        }
        return null;
    }

    // ── GET /api/jobs ─────────────────────────────────────────────────────

    @GetMapping("/jobs")
    public Map<String, Object> listJobs(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "20") int size) {
        var jobRepo = service.jobRepository();
        var jobs = jobRepo.listJobs(page, size);
        int total = jobRepo.countJobs();

        // Attach live progress to EVERY currently-running job so each row in Job
        // History shows an accurate progress bar — multiple jobs can run concurrently.
        Map<Long, JobStatus> runningStatuses = service.allRunningJobStatuses();
        if (!runningStatuses.isEmpty()) {
            jobs = jobs.stream().map(j -> {
                Object rawId = j.get("id");
                if (!(rawId instanceof Number)) return j;
                long jid = ((Number) rawId).longValue();
                JobStatus live = runningStatuses.get(jid);
                if (live == null) return j;
                Map<String, Object> enriched = new LinkedHashMap<>(j);
                enriched.put("status", "RUNNING");
                enriched.put("sourceCount", live.sourceCount());
                enriched.put("destCount", live.destCount());
                enriched.put("diffCount", live.diffCount());
                enriched.put("progressPercent", live.percentComplete());
                return enriched;
            }).toList();
        }

        return Map.of("rows", jobs, "total", total, "page", page, "pageSize", size);
    }

    // ── GET /api/jobs/{id}/status ─────────────────────────────────────────

    /**
     * Returns the live status for a specific job.
     * If the job is still RUNNING the response comes from the in-memory
     * {@code AtomicReference} (updated every ~500 ms); once the job finishes
     * the response falls back to the persisted DB row.
     */
    @GetMapping("/jobs/{id}/status")
    public ResponseEntity<Map<String, Object>> getJobStatus(@PathVariable long id) {
        JobStatus live = service.jobStatus(id);
        if (live != null) {
            return ResponseEntity.ok(Map.of(
                    "jobId", id,
                    "status", live.status(),
                    "sourceCount", live.sourceCount(),
                    "destCount", live.destCount(),
                    "diffCount", live.diffCount(),
                    "progressPercent", live.percentComplete(),
                    "error", live.error() != null ? live.error() : ""
            ));
        }
        // Job not in the live map — read from DB (already completed/failed)
        var jobRepo = service.jobRepository();
        var job = jobRepo.getJob(id);
        if (job == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "Job not found"));
        }
        return ResponseEntity.ok(Map.of(
                "jobId", id,
                "status", job.getOrDefault("status", "UNKNOWN"),
                "sourceCount", job.getOrDefault("sourceCount", 0L),
                "destCount", job.getOrDefault("destCount", 0L),
                "diffCount", job.getOrDefault("diffCount", 0L),
                "progressPercent", -1,
                "error", job.getOrDefault("errorMsg", "")
        ));
    }

    // ── GET /api/jobs/{id} ────────────────────────────────────────────────

    @GetMapping("/jobs/{id}")
    public ResponseEntity<Map<String, Object>> getJob(@PathVariable long id) {
        var jobRepo = service.jobRepository();
        var job = jobRepo.getJob(id);
        if (job == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "Job not found"));
        }
        var tables = jobRepo.getJobTables(id);
        // Attach live per-table progress so a running table shows an accurate
        // progress bar instead of just a static "RUNNING" badge — otherwise
        // smaller finished tables in the same job (already at 100%) can be
        // confused with a much bigger table that's still mid-scan.
        tables = tables.stream().map(t -> {
            if ("RUNNING".equals(t.get("status"))) {
                Object rawId = t.get("id");
                if (!(rawId instanceof Number)) return t;
                JobStatus live = service.liveTableStatus(((Number) rawId).longValue());
                if (live == null) return t;
                Map<String, Object> enriched = new LinkedHashMap<>(t);
                enriched.put("sourceCount", live.sourceCount());
                enriched.put("destCount", live.destCount());
                enriched.put("diffCount", live.diffCount());
                enriched.put("progressPercent", live.percentComplete());
                return enriched;
            }
            // For FAILED tables (e.g. interrupted by an app restart), compute
            // "how far did it get" from the counts persisted at the last
            // checkpoint — those columns used to only ever be written once,
            // at successful completion, so an interrupted table always showed
            // 0/0/0 with no indication of its actual progress.
            if ("FAILED".equals(t.get("status"))) {
                long totalEstimate = toLong(t.get("totalEstimate"));
                if (totalEstimate <= 0) return t;
                JobStatus snapshot = new JobStatus("FAILED", toLong(t.get("sourceCount")),
                        toLong(t.get("destCount")), toLong(t.get("diffCount")), null, totalEstimate);
                Map<String, Object> enriched = new LinkedHashMap<>(t);
                enriched.put("progressPercent", snapshot.percentComplete());
                return enriched;
            }
            return t;
        }).toList();
        Map<String, Object> out = new LinkedHashMap<>(job);
        out.put("tables", tables);
        return ResponseEntity.ok(out);
    }

    private static long toLong(Object o) {
        return o instanceof Number n ? n.longValue() : 0L;
    }

    // ── GET /api/jobs/{id}/diffs ──────────────────────────────────────────

    @GetMapping("/jobs/{id}/diffs")
    public ResponseEntity<Map<String, Object>> getJobDiffs(
            @PathVariable long id,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "100") int size,
            @RequestParam(required = false) String table,
            @RequestParam(required = false) String search) {
        var jobRepo = service.jobRepository();
        var job = jobRepo.getJob(id);
        if (job == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "Job not found"));
        }
        String jobTs = (String) job.get("jobTs");
        var diffs = diffRepo.fetchByJobTs(jobTs, page, size, search, table);
        int total = diffRepo.countByJobTs(jobTs, search, table);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("jobId", id);
        out.put("jobTs", jobTs);
        out.put("rows", diffs);
        out.put("totalRows", total);
        out.put("page", page);
        out.put("pageSize", size);
        return ResponseEntity.ok(out);
    }

    // ── DELETE /api/jobs/{id} ─────────────────────────────────────────────

    @DeleteMapping("/jobs/{id}")
    public ResponseEntity<Map<String, String>> deleteJob(@PathVariable long id) {
        var jobRepo = service.jobRepository();
        var job = jobRepo.getJob(id);
        if (job == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "Job not found"));
        }
        // If the job is still RUNNING, request cancellation FIRST so the in-flight
        // CompareEngine thread(s) actually stop (checked every loop iteration) —
        // otherwise deleting only the DB rows leaves the scan running invisibly in
        // the background, still writing diffs and hitting Oracle/Snowflake.
        boolean wasRunning = "RUNNING".equals(job.get("status"));
        boolean cancelled = wasRunning && service.cancelJob(id);
        if (wasRunning) {
            log.info("Deleting RUNNING job #{} — cancellation {}", id, cancelled ? "requested" : "requested but no live thread found (already finishing)");
        }

        String jobTs = (String) job.get("jobTs");
        diffRepo.clearByJobTs(jobTs);
        jobRepo.deleteJob(id);
        String message = wasRunning
                ? "Job and its diffs deleted (cancellation requested for the running scan)"
                : "Job and its diffs deleted";
        return ResponseEntity.ok(Map.of("message", message));
    }
}

