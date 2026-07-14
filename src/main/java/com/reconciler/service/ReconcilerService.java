package com.reconciler.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.reconciler.config.AppProperties;
import com.reconciler.config.BatchRunRequest;
import com.reconciler.config.JobConfig;
import com.reconciler.engine.CompareEngine;
import com.reconciler.engine.QueryStrategy;
import com.reconciler.engine.SchemaInspector;
import com.reconciler.engine.ValueNormalizer;
import com.reconciler.model.JobStatus;
import com.reconciler.repository.DiffRepository;
import com.reconciler.repository.JobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Manages the lifecycle of a single reconciliation job.
 * Exposes start/status operations consumed by {@link com.reconciler.controller.ReconcilerController}.
 */
@Service
public class ReconcilerService {

    private static final Logger log = LoggerFactory.getLogger(ReconcilerService.class);

    /**
     * Per-job live status refs: jobId → AtomicReference&lt;JobStatus&gt;.
     * An entry is present while the job is RUNNING; removed in the worker's
     * finally block when the job finishes (success / failure / cancel).
     * Multiple jobs can be present simultaneously — there is no single-job gate.
     */
    private final ConcurrentHashMap<Long, AtomicReference<JobStatus>> activeJobStatuses = new ConcurrentHashMap<>();

    /**
     * Snapshot of the most recently completed (or failed/cancelled) job status.
     * Returned by {@link #currentStatus()} when no jobs are actively running,
     * so callers always see the last meaningful result rather than IDLE.
     */
    private final AtomicReference<JobStatus> lastCompletedStatus = new AtomicReference<>(JobStatus.idle());

    /** DB id of the most recently *started* job. Used by the UI progress bar
     *  as a default when no jobId is specified. */
    private final java.util.concurrent.atomic.AtomicLong latestStartedJobId = new java.util.concurrent.atomic.AtomicLong(-1);

    private final AtomicReference<JobConfig> lastConfig = new AtomicReference<>();
    private final Map<String, JobConfig> tableConfigs = new ConcurrentHashMap<>();
    /** Per-table scanned row counts: tableName (upper) -> [sourceCount, destCount] */
    private final ConcurrentHashMap<String, long[]> tableRowCounts = new ConcurrentHashMap<>();
    /**
     * DB job_table row id -> live {@link JobStatus} reference for that specific
     * table's {@link CompareEngine} run.
     */
    private final ConcurrentHashMap<Long, AtomicReference<JobStatus>> liveTableStatuses = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    /** DB jobId -> cancellation flag, shared with every CompareEngine running under that job. */
    private final ConcurrentHashMap<Long, AtomicBoolean> cancelFlags = new ConcurrentHashMap<>();

    private final DiffRepository diffRepo;
    private final JobRepository  jobRepo;
    private final AppProperties  props;

    public ReconcilerService(DiffRepository diffRepo, JobRepository jobRepo, AppProperties props) {
        this.diffRepo = diffRepo;
        this.jobRepo  = jobRepo;
        this.props    = props;

        int recovered = jobRepo.recoverStaleRunningJobs();
        if (recovered > 0) {
            log.warn("Recovered {} stale RUNNING job/table row(s) from a previous run " +
                    "(likely interrupted by an app restart) — marked as FAILED.", recovered);
        }
        // NOTE (L2): auto-resume runs from onApplicationReady(), not here.
    }

    @org.springframework.context.event.EventListener(org.springframework.boot.context.event.ApplicationReadyEvent.class)
    public void onApplicationReady() {
        autoResumeInterruptedTables();
    }

    /**
     * Aggregate live status across ALL currently-running jobs.
     * Returns summed counts with status RUNNING if any job is active,
     * or the last completed job's status if none are running.
     */
    public JobStatus currentStatus() {
        if (activeJobStatuses.isEmpty()) {
            return lastCompletedStatus.get();
        }
        long src = 0, dest = 0, diff = 0, total = 0;
        for (AtomicReference<JobStatus> ref : activeJobStatuses.values()) {
            JobStatus s = ref.get();
            src   += s.sourceCount();
            dest  += s.destCount();
            diff  += s.diffCount();
            total += s.totalEstimate();
        }
        return new JobStatus("RUNNING", src, dest, diff, null, total);
    }

    /**
     * Live status for a specific job, or {@code null} if that job is not
     * currently active (not started, or already finished).
     */
    public JobStatus jobStatus(long jobId) {
        AtomicReference<JobStatus> ref = activeJobStatuses.get(jobId);
        return ref == null ? null : ref.get();
    }

    /** DB id of the most recently started job, or {@code -1} if none. */
    public Long currentJobId() {
        long id = latestStartedJobId.get();
        return id < 0 ? null : id;
    }

    public JobRepository jobRepository() {
        return jobRepo;
    }

    /**
     * Requests cancellation of a running job.
     * @return {@code true} if a live job with this id was found and signalled.
     */
    public boolean cancelJob(long jobId) {
        AtomicBoolean flag = cancelFlags.get(jobId);
        if (flag == null) {
            return false;
        }
        flag.set(true);
        log.warn("Cancellation requested for job #{}", jobId);
        return true;
    }

    /**
     * Starts a reconciliation job in a virtual thread. Always starts immediately —
     * multiple jobs can run concurrently.
     *
     * @return the DB job id of the newly started job.
     */
    public long startJob(JobConfig config) {
        JobConfig sanitized = sanitizeConfig(config);
        // Register the config so diffDetails() can look it up by table name.
        tableConfigs.put(tailName(sanitized.sourceTable()), sanitized);
        tableConfigs.put(sanitized.sourceTable().trim().toUpperCase(Locale.ROOT), sanitized);
        lastConfig.set(sanitized);

        // Create the DB job row synchronously so we can return the jobId immediately.
        String jobTs = java.time.Instant.now().toString();
        String tableName = sanitized.sourceTable().trim().toUpperCase(Locale.ROOT);
        long jobId = jobRepo.createJob(jobTs, 1);
        latestStartedJobId.set(jobId);

        AtomicReference<JobStatus> jobStatus = new AtomicReference<>(JobStatus.running());
        activeJobStatuses.put(jobId, jobStatus);

        AtomicBoolean cancelFlag = new AtomicBoolean(false);
        cancelFlags.put(jobId, cancelFlag);
        long jobTableId = jobRepo.createJobTable(jobId, jobTs, tableName, toJson(sanitized));
        jobRepo.markTableRunning(jobTableId);
        liveTableStatuses.put(jobTableId, jobStatus);

        log.info("Starting reconciliation job #{}: {}", jobId, sanitized);

        Thread.ofVirtual().name("reconciler-job-" + jobId).start(() -> {
            try {
                CompareEngine.CheckpointCallback checkpointCallback = (pk, src, dest, diff, totalEstimate) ->
                        jobRepo.updateCheckpoint(jobTableId, pk, src, dest, diff, totalEstimate);
                new CompareEngine(sanitized, props, diffRepo, jobStatus, jobTs, null, checkpointCallback, cancelFlag).run();
                JobStatus done = jobStatus.get();
                String fullKey = sanitized.sourceTable().trim().toUpperCase(Locale.ROOT);
                String tailKey = tailName(sanitized.sourceTable()).toUpperCase(Locale.ROOT);
                long[] counts = new long[]{done.sourceCount(), done.destCount()};
                tableRowCounts.put(fullKey, counts);
                tableRowCounts.put(tailKey, counts);
                String finalStatus = "CANCELLED".equals(done.status()) ? "FAILED" : "COMPLETED";
                jobRepo.completeTable(jobTableId, finalStatus, done.sourceCount(), done.destCount(), done.diffCount(), done.error());
                jobRepo.updateJob(jobId, finalStatus, done.sourceCount(), done.destCount(), done.diffCount(), done.error());
                lastCompletedStatus.set(done);
            } catch (Exception ex) {
                log.error("Reconciliation job #{} failed", jobId, ex);
                JobStatus lastKnown = jobStatus.get();
                long src = lastKnown != null ? lastKnown.sourceCount() : 0;
                long dest = lastKnown != null ? lastKnown.destCount() : 0;
                long diff = lastKnown != null ? lastKnown.diffCount() : 0;
                JobStatus failed = new JobStatus("FAILED", src, dest, diff, ex.getMessage());
                jobStatus.set(failed);
                lastCompletedStatus.set(failed);
                jobRepo.completeTable(jobTableId, "FAILED", src, dest, diff, ex.getMessage());
                jobRepo.updateJob(jobId, "FAILED", src, dest, diff, ex.getMessage());
            } finally {
                cancelFlags.remove(jobId);
                activeJobStatuses.remove(jobId);
            }
        });
        return jobId;
    }

    /**
     * Starts reconciliation for all/selected tables in a schema pair.
     * Always starts immediately — multiple batch jobs can run concurrently.
     *
     * @return the DB job id of the newly started batch job.
     */
    public long startBatch(BatchRunRequest request) {
        // Create the DB job row synchronously so we can return the jobId immediately.
        String jobTs = java.time.Instant.now().toString();
        // Placeholder row — will be updated once we know the real table count.
        long jobId = jobRepo.createJob(jobTs, 0);
        latestStartedJobId.set(jobId);

        AtomicReference<JobStatus> jobStatus = new AtomicReference<>(JobStatus.running());
        activeJobStatuses.put(jobId, jobStatus);

        AtomicBoolean cancelFlag = new AtomicBoolean(false);
        cancelFlags.put(jobId, cancelFlag);

        log.info("Starting batch job #{}", jobId);

        Thread.ofVirtual().name("reconciler-batch-" + jobId).start(() -> {
            try {
                if (request.clearBeforeEffective()) {
                    diffRepo.clear();
                }

                List<ResolvedTable> tables = resolveTables(request);
                if (tables.isEmpty()) {
                    JobStatus failed = new JobStatus("FAILED", 0, 0, 0, "No matching tables found");
                    jobStatus.set(failed);
                    lastCompletedStatus.set(failed);
                    jobRepo.updateJob(jobId, "FAILED", 0, 0, 0, "No matching tables found");
                    return;
                }

                log.info("Batch job #{} resolved {} table(s): {}", jobId, tables.size(),
                        tables.stream().map(ResolvedTable::scopedKey).toList());

                List<JobConfig> runnableConfigs = new ArrayList<>();
                List<String> skipped = new ArrayList<>();
                for (ResolvedTable table : tables) {
                    try {
                        JobConfig cfg = sanitizeConfig(buildJobConfigForTable(request, table));
                        runnableConfigs.add(cfg);
                        tableConfigs.put(table.scopedKey(), cfg);
                        tableConfigs.putIfAbsent(tailName(cfg.sourceTable()), cfg);
                        tableConfigs.put(cfg.sourceTable().trim().toUpperCase(Locale.ROOT), cfg);
                    } catch (Exception ex) {
                        log.warn("Skipping table {} in batch pre-validation: {}", table.scopedKey(), ex.getMessage());
                        skipped.add(table.scopedKey() + " (" + ex.getMessage() + ")");
                    }
                }

                if (runnableConfigs.isEmpty()) {
                    String msg = "No tables processed. " + String.join("; ", skipped);
                    JobStatus failed = new JobStatus("FAILED", 0, 0, 0, msg);
                    jobStatus.set(failed);
                    lastCompletedStatus.set(failed);
                    jobRepo.updateJob(jobId, "FAILED", 0, 0, 0, msg);
                    return;
                }

                // Update the job row with the real table count now that we know it.
                jobRepo.updateJobTableCount(jobId, runnableConfigs.size());

                Map<JobConfig, Long> jobTableIds = new ConcurrentHashMap<>();
                for (JobConfig cfg : runnableConfigs) {
                    String tblName = cfg.sourceTable().trim().toUpperCase(Locale.ROOT);
                    long jtId = jobRepo.createJobTable(jobId, jobTs, tblName, toJson(cfg));
                    jobTableIds.put(cfg, jtId);
                }

                BatchResult result = runTablesInParallel(runnableConfigs, jobTableIds, jobTs, skipped, Map.of(), cancelFlag, Map.of(), jobStatus);
                long totalSrc = result.totalSrc();
                long totalDest = result.totalDest();
                long totalDiff = result.totalDiff();

                if (totalSrc == 0 && totalDest == 0 && totalDiff == 0 && !skipped.isEmpty()) {
                    String msg = "No tables processed. " + String.join("; ", skipped);
                    JobStatus failed = new JobStatus("FAILED", 0, 0, 0, msg);
                    jobStatus.set(failed);
                    lastCompletedStatus.set(failed);
                    jobRepo.updateJob(jobId, "FAILED", 0, 0, 0, msg);
                    return;
                }

                boolean cancelledMidway = cancelFlag.get();
                String message = skipped.isEmpty() ? null : "Skipped " + skipped.size() + " table(s): " + String.join("; ", skipped);
                String finalStatus = cancelledMidway ? "FAILED" : "COMPLETED";
                if (cancelledMidway) {
                    message = "Cancelled by user" + (message != null ? "; " + message : "");
                }
                JobStatus finalJobStatus = new JobStatus(finalStatus, totalSrc, totalDest, totalDiff, message);
                jobStatus.set(finalJobStatus);
                lastCompletedStatus.set(finalJobStatus);
                jobRepo.updateJob(jobId, finalStatus, totalSrc, totalDest, totalDiff, message);
                log.info("Batch job #{} complete. src={} dest={} diff={} skipped={} cancelled={}",
                        jobId, totalSrc, totalDest, totalDiff, skipped.size(), cancelledMidway);
            } catch (Exception ex) {
                log.error("Batch job #{} failed", jobId, ex);
                long[] sums = jobRepo.sumJobTableCounts(jobId);
                JobStatus failed = new JobStatus("FAILED", sums[0], sums[1], sums[2], ex.getMessage());
                jobStatus.set(failed);
                lastCompletedStatus.set(failed);
                jobRepo.updateJob(jobId, "FAILED", sums[0], sums[1], sums[2], ex.getMessage());
            } finally {
                cancelFlags.remove(jobId);
                activeJobStatuses.remove(jobId);
            }
        });

        return jobId;
    }

    /**
     * Drops known volatile columns from the hash list to avoid false-positive
     * HASH_MISMATCH events between Oracle and Snowflake CDC windows.
     *
     * <p>The modified timestamp column is frequently updated by pipelines,
     * precision-normalized differently per database, and is already used as the
     * high-watermark buffer filter, so hashing it usually creates noise.
     */
    private JobConfig sanitizeConfig(JobConfig cfg) {
        if (cfg == null) return null;
        if (cfg.hashColumns() == null || cfg.hashColumns().isEmpty()) return cfg;
        if (cfg.modifiedColumn() == null || cfg.modifiedColumn().isBlank()) return cfg;

        String mod = cfg.modifiedColumn().trim();
        List<String> filtered = new ArrayList<>();
        for (String c : cfg.hashColumns()) {
            if (c == null) continue;
            if (!c.trim().equalsIgnoreCase(mod)) {
                filtered.add(c.trim());
            }
        }

        if (filtered.size() != cfg.hashColumns().size()) {
            log.warn("Removed modifiedColumn '{}' from hashColumns to reduce CDC false positives", mod);
        }

        return new JobConfig(
                cfg.sourceTable(),
                cfg.destTable(),
                cfg.primaryKeys(),
                filtered,
                cfg.modifiedColumn(),
                cfg.bufferMinutes(),
                cfg.destFilterColumn(),
                cfg.destFilterValue()
        );
    }

    /** Reset status to IDLE (called after clearing results). */
    public void resetStatus() {
        lastCompletedStatus.set(JobStatus.idle());
        tableConfigs.clear();
        tableRowCounts.clear();
        liveTableStatuses.clear();
    }

    /** Returns per-table scanned row counts: map of tableName -> {sourceCount, destCount}. */
    public Map<String, long[]> tableRowCounts() {
        return Collections.unmodifiableMap(tableRowCounts);
    }

    /**
     * Returns all currently-running job IDs and their live statuses.
     */
    public Map<Long, JobStatus> allRunningJobStatuses() {
        Map<Long, JobStatus> out = new java.util.LinkedHashMap<>();
        activeJobStatuses.forEach((id, ref) -> out.put(id, ref.get()));
        return out;
    }

    /**
     * Live progress for a specific job_table row (by its DB id), or {@code null}
     * if that table isn't currently tracked (not running / job finished and its
     * live reference was superseded by a newer run).
     */
    public JobStatus liveTableStatus(long jobTableId) {
        AtomicReference<JobStatus> ref = liveTableStatuses.get(jobTableId);
        return ref == null ? null : ref.get();
    }

    /**
     * Returns per-column value differences for a specific composite PK.
     * Response rows include: column, oracleValue, snowflakeValue, different.
     */
    public List<Map<String, Object>> diffDetails(String pkComposite, String tableName) throws SQLException {
        JobConfig cfg = null;
        if (tableName != null && !tableName.isBlank()) {
            String lookup = tableName.trim().toUpperCase(Locale.ROOT);
            cfg = tableConfigs.get(lookup);
            if (cfg == null) {
                cfg = tableConfigs.get(tailName(lookup));
            }
        }
        if (cfg == null) {
            cfg = lastConfig.get();
        }
        if (cfg == null) {
            throw new IllegalStateException("No job config available. Run a job first.");
        }

        List<String> keyCols = cfg.keyColumns();
        String[] keyVals = pkComposite.split("~", -1);
        if (keyVals.length != keyCols.size()) {
            throw new IllegalArgumentException("PK composite does not match configured key columns");
        }

        LinkedHashSet<String> compareSet = new LinkedHashSet<>();
        if (cfg.hashColumns() != null && !cfg.hashColumns().isEmpty()) compareSet.addAll(cfg.hashColumns());
        else compareSet.addAll(keyCols);
        if (cfg.modifiedColumn() != null && !cfg.modifiedColumn().isBlank()) {
            compareSet.add(cfg.modifiedColumn().trim());
        }
        List<String> compareCols = new ArrayList<>(compareSet);

        try (Connection src = DriverManager.getConnection(
                props.oracle().jdbcUrl(), props.oracle().user(), props.oracle().password());
             Connection dst = DriverManager.getConnection(
                     props.snowflake().jdbcUrl(), props.snowflake().user(), props.snowflake().password())) {

            Map<String, String> o = fetchRowAsStrings(src, cfg.sourceTable(), keyCols, keyVals, compareCols, true, null, null);
            Map<String, String> s = fetchRowAsStrings(dst, cfg.destTable(), keyCols, keyVals, compareCols, false,
                    cfg.destFilterColumn(), cfg.destFilterValue());

            List<QueryStrategy.ColumnInfo> srcCols = loadColumnInfo(src, cfg.sourceTable());
            List<QueryStrategy.ColumnInfo> dstCols = loadColumnInfo(dst, cfg.destTable());

            List<Map<String, Object>> out = new ArrayList<>();
            for (String c : compareCols) {
                String key = c.toUpperCase(Locale.ROOT);
                String srcType = srcCols.stream()
                        .filter(ci -> ci.name().equalsIgnoreCase(c))
                        .map(QueryStrategy.ColumnInfo::dataType)
                        .findFirst().orElse("");
                String dstType = dstCols.stream()
                        .filter(ci -> ci.name().equalsIgnoreCase(c))
                        .map(QueryStrategy.ColumnInfo::dataType)
                        .findFirst().orElse("");
                String type = !srcType.isBlank() ? srcType : dstType;

                String ov = ValueNormalizer.normalize(o.getOrDefault(key, ""), type);
                String sv = ValueNormalizer.normalize(s.getOrDefault(key, ""), type);
                out.add(Map.of(
                        "column", c,
                        "oracleValue", ov,
                        "snowflakeValue", sv,
                        "different", !Objects.equals(ov, sv)
                ));
            }
            return out;
        }
    }

    private List<QueryStrategy.ColumnInfo> loadColumnInfo(Connection conn, String tableRef) throws SQLException {
        String[] parts = tableRef.toUpperCase(Locale.ROOT).split("\\.");
        String catalog = null;
        String schema;
        String table;

        if (parts.length >= 3) {
            catalog = parts[parts.length - 3];
            schema = parts[parts.length - 2];
            table = parts[parts.length - 1];
        } else if (parts.length == 2) {
            schema = parts[0];
            table = parts[1];
        } else {
            schema = null;
            table = parts[0];
        }

        List<QueryStrategy.ColumnInfo> out = new ArrayList<>();
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(catalog, schema, table, null)) {
            while (rs.next()) {
                out.add(new QueryStrategy.ColumnInfo(
                        rs.getString("COLUMN_NAME"),
                        rs.getString("TYPE_NAME")
                ));
            }
        }
        return out;
    }

    private Map<String, String> fetchRowAsStrings(Connection conn,
                                                  String table,
                                                  List<String> keyCols,
                                                  String[] keyVals,
                                                  List<String> outCols,
                                                  boolean oracle,
                                                  String destFilterColumn,
                                                  String destFilterValue) throws SQLException {
        String selectCols = outCols.stream()
                .map(c -> (oracle
                        ? "CAST(" + c + " AS VARCHAR2(4000))"
                        : "CAST(" + c + " AS VARCHAR(4000))") + " AS " + c)
                .reduce((a, b) -> a + ", " + b)
                .orElse("1");

        String where = keyCols.stream().map(k -> k + " = ?")
                .reduce((a, b) -> a + " AND " + b).orElse("1=1");

        if (!oracle && destFilterColumn != null && !destFilterColumn.isBlank()
                && destFilterValue != null && !destFilterValue.isBlank()) {
            where += " AND " + safeIdentifier(destFilterColumn) + " = ?";
        }

        String sql = "SELECT " + selectCols + " FROM " + table + " WHERE " + where
                + (oracle ? " FETCH FIRST 1 ROWS ONLY" : " LIMIT 1");

        Map<String, String> out = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (String keyVal : keyVals) ps.setString(idx++, keyVal);
            if (!oracle && destFilterColumn != null && !destFilterColumn.isBlank()
                    && destFilterValue != null && !destFilterValue.isBlank()) {
                ps.setString(idx, destFilterValue);
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    for (String c : outCols) {
                        out.put(c.toUpperCase(Locale.ROOT), Objects.toString(rs.getString(c), ""));
                    }
                }
            }
        }
        return out;
    }

    private List<ResolvedTable> resolveTables(BatchRunRequest req) throws SQLException {
        if (req.hasSourceSchemas()) {
            return resolveLegacyTablesForSchemas(req, req.selectedSourceSchemas());
        }
        if (req.hasSchemaIds() || !props.enabledMappings().isEmpty()) {
            return resolveTablesFromMappings(req);
        }
        return resolveLegacyTables(req);
    }

    private List<ResolvedTable> resolveLegacyTablesForSchemas(BatchRunRequest req, List<String> sourceSchemas) throws SQLException {
        if (req.destSchema() == null || req.destSchema().isBlank()) {
            throw new IllegalArgumentException("destSchema is required when sourceSchemas are provided");
        }

        List<SchemaInspector.TableRef> snow = SchemaInspector.listSnowflakeTables(req.destDatabase(), req.destSchema(), "", props);
        Map<String, String> sByName = snow.stream().collect(Collectors.toMap(
                t -> tailName(t.qualifiedName()), SchemaInspector.TableRef::qualifiedName, (a, b) -> a));

        HashSet<String> requested = new HashSet<>(requestedTables(req));
        List<ResolvedTable> out = new ArrayList<>();

        for (String srcSchema : sourceSchemas) {
            List<SchemaInspector.TableRef> oracle = SchemaInspector.listOracleTables(srcSchema, "", props);
            Map<String, String> oByName = oracle.stream().collect(Collectors.toMap(
                    t -> tailName(t.qualifiedName()), SchemaInspector.TableRef::qualifiedName, (a, b) -> a));

            List<String> names = oByName.keySet().stream().filter(sByName::containsKey).sorted().toList();
            for (String table : names) {
                if (!requested.isEmpty() && !requested.contains(table)) {
                    continue;
                }
                out.add(new ResolvedTable(srcSchema.toUpperCase(Locale.ROOT), table,
                        oByName.get(table), sByName.get(table), null, null));
            }
        }

        out.sort((a, b) -> a.scopedKey().compareTo(b.scopedKey()));
        return out;
    }

    private List<ResolvedTable> resolveLegacyTables(BatchRunRequest req) throws SQLException {
        if (req.sourceSchema() == null || req.sourceSchema().isBlank()
                || req.destSchema() == null || req.destSchema().isBlank()) {
            throw new IllegalArgumentException("sourceSchema and destSchema are required when schema mappings are not configured");
        }

        List<SchemaInspector.TableRef> oracle = SchemaInspector.listOracleTables(req.sourceSchema(), "", props);
        List<SchemaInspector.TableRef> snow = SchemaInspector.listSnowflakeTables(req.destDatabase(), req.destSchema(), "", props);

        Map<String, String> oByName = oracle.stream().collect(Collectors.toMap(
                t -> tailName(t.qualifiedName()), SchemaInspector.TableRef::qualifiedName, (a, b) -> a));
        Map<String, String> sByName = snow.stream().collect(Collectors.toMap(
                t -> tailName(t.qualifiedName()), SchemaInspector.TableRef::qualifiedName, (a, b) -> a));

        List<String> requested = requestedTables(req);
        List<String> names = requested.isEmpty()
                ? oByName.keySet().stream().filter(sByName::containsKey).sorted().toList()
                : requested.stream().map(this::tailName).filter(n -> oByName.containsKey(n) && sByName.containsKey(n)).toList();

        return names.stream()
                .map(name -> new ResolvedTable("DEFAULT", name, oByName.get(name), sByName.get(name), null, null))
                .toList();
    }

    private List<ResolvedTable> resolveTablesFromMappings(BatchRunRequest req) throws SQLException {
        Map<String, AppProperties.SchemaMapping> byId = props.mappingById();
        if (byId.isEmpty()) {
            throw new IllegalArgumentException("No schema mappings configured under reconciler.mappings");
        }

        List<String> selectedIds = req.hasSchemaIds()
                ? req.schemaIds().stream().filter(Objects::nonNull).map(String::trim)
                .filter(s -> !s.isBlank()).map(s -> s.toUpperCase(Locale.ROOT)).distinct().toList()
                : props.enabledMappings().stream().map(AppProperties.SchemaMapping::normalizedSchemaId).toList();

        if (selectedIds.isEmpty()) {
            throw new IllegalArgumentException("No schemaIds selected for batch run");
        }

        HashSet<String> requested = new HashSet<>(requestedTables(req));
        List<ResolvedTable> out = new ArrayList<>();

        for (String schemaId : selectedIds) {
            AppProperties.SchemaMapping mapping = byId.get(schemaId);
            if (mapping == null) {
                throw new IllegalArgumentException("Unknown schemaId: " + schemaId);
            }
            if (!mapping.enabled()) {
                log.info("Skipping disabled schema mapping [{}]", schemaId);
                continue;
            }
            if (!mapping.isUsable()) {
                log.warn("Skipping incomplete schema mapping [{}]", schemaId);
                continue;
            }

            List<SchemaInspector.TableRef> oracle = SchemaInspector.listOracleTables(mapping.oracleSchema(), "", props);
            List<SchemaInspector.TableRef> snow = SchemaInspector.listSnowflakeTables(
                    blankToNull(mapping.snowflakeDatabase()), mapping.snowflakeSchema(), "", props);

            Map<String, String> oByName = oracle.stream().collect(Collectors.toMap(
                    t -> tailName(t.qualifiedName()), SchemaInspector.TableRef::qualifiedName, (a, b) -> a));
            Map<String, String> sByName = snow.stream().collect(Collectors.toMap(
                    t -> tailName(t.qualifiedName()), SchemaInspector.TableRef::qualifiedName, (a, b) -> a));

            List<String> names = oByName.keySet().stream().filter(sByName::containsKey).sorted().toList();
            for (String table : names) {
                if (!requested.isEmpty() && !requested.contains(table)) {
                    continue;
                }
                out.add(new ResolvedTable(
                        schemaId,
                        table,
                        oByName.get(table),
                        sByName.get(table),
                        mapping.destFilterColumn(),
                        mapping.destFilterValue()
                ));
            }
        }

        out.sort((a, b) -> a.scopedKey().compareTo(b.scopedKey()));
        return out;
    }

    private List<String> requestedTables(BatchRunRequest req) {
        if (req.tables() == null || req.tables().isEmpty()) {
            return List.of();
        }
        return req.tables().stream().filter(Objects::nonNull)
                .map(String::trim).filter(s -> !s.isBlank())
                .map(this::tailName)
                .distinct()
                .toList();
    }

    private JobConfig buildJobConfigForTable(BatchRunRequest req, ResolvedTable resolved) throws SQLException {
        String srcTable = resolved.sourceQualified();
        String dstTable = resolved.destQualified();
        BatchRunRequest.TableConfigOverride override = req.configForTable(resolved.tableName());

        var srcSchema = SchemaInspector.fromOracle(srcTable, props);
        List<String> pks = override != null && override.primaryKeys() != null && !override.primaryKeys().isBlank()
                ? List.of(override.primaryKeys().split("~"))
                : srcSchema.primaryKeys();
        if (pks == null || pks.isEmpty()) {
            throw new IllegalStateException("Table has no primary key: " + srcTable);
        }

        List<String> hashCols = override != null && override.hashColumns() != null && !override.hashColumns().isEmpty()
                ? override.hashColumns()
                : srcSchema.columns().stream().map(SchemaInspector.ColumnMeta::name).toList();

        String modifiedColumn = override != null && override.modifiedColumn() != null && !override.modifiedColumn().isBlank()
                ? override.modifiedColumn()
                : req.modifiedColumn();

        Integer bufferMinutes = override != null && override.bufferMinutes() != null
                ? override.bufferMinutes()
                : req.effectiveBufferMinutes();

        return new JobConfig(
                srcTable,
                dstTable,
                String.join("~", pks),
                hashCols,
                modifiedColumn,
                bufferMinutes,
                resolved.destFilterColumn(),
                resolved.destFilterValue()
        );
    }

    private String safeIdentifier(String ident) {
        String upper = ident == null ? "" : ident.trim().toUpperCase(Locale.ROOT);
        if (!upper.matches("[A-Z0-9_\\$]+")) {
            throw new IllegalArgumentException("Unsupported identifier format: " + ident);
        }
        return upper;
    }

    private String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s;
    }

    private String tailName(String qualified) {
        int dot = qualified.lastIndexOf('.');
        return dot >= 0 ? qualified.substring(dot + 1).toUpperCase(Locale.ROOT) : qualified.toUpperCase(Locale.ROOT);
    }

    private TableRunResult runSingleTable(JobConfig cfg, String jobTs, AtomicReference<JobStatus> liveStatus) {
        return runSingleTable(cfg, jobTs, liveStatus, null, null, null, null);
    }

    /**
     * @param resumeAfterPk if non-null, restarts the scan past this checkpoint
     *                      instead of from the beginning (see {@link CompareEngine}).
     * @param jobTableId    if non-null, wires a checkpoint callback so progress
     *                      is persisted periodically and can be resumed later.
     * @param cancelFlag    if non-null, shared cancellation flag checked by
     *                      {@link CompareEngine} every loop iteration.
     * @param resumeBaseCounts if non-null, {@code {sourceCount, destCount, diffCount}}
     *                      already accumulated by the PRIOR interrupted run of this
     *                      same table — carried forward as the starting point so the
     *                      final counts reflect the WHOLE table, not just the rows
     *                      processed since the restart.
     */
    private TableRunResult runSingleTable(JobConfig cfg, String jobTs, AtomicReference<JobStatus> liveStatus,
                                          String resumeAfterPk, Long jobTableId, AtomicBoolean cancelFlag,
                                          long[] resumeBaseCounts) {
        String table = tailName(cfg.sourceTable());
        long startMs = System.currentTimeMillis();
        log.info("Batch table start [{}] src={} dst={}{}", table, cfg.sourceTable(), cfg.destTable(),
                resumeAfterPk != null ? " (resuming past checkpoint)" : "");
        long baseSrc = resumeBaseCounts != null ? resumeBaseCounts[0] : 0;
        long baseDest = resumeBaseCounts != null ? resumeBaseCounts[1] : 0;
        long baseDiff = resumeBaseCounts != null ? resumeBaseCounts[2] : 0;
        try {
            CompareEngine.CheckpointCallback checkpointCallback = jobTableId == null ? null
                    : (pk, src, dest, diff, totalEstimate) -> jobRepo.updateCheckpoint(jobTableId, pk, src, dest, diff, totalEstimate);
            new CompareEngine(cfg, props, diffRepo, liveStatus, jobTs, resumeAfterPk, checkpointCallback, cancelFlag,
                    baseSrc, baseDest, baseDiff).run();
            JobStatus done = liveStatus.get();
            long elapsed = System.currentTimeMillis() - startMs;
            log.info("Batch table end [{}] in {} ms{}", table, elapsed,
                    "CANCELLED".equals(done.status()) ? " (cancelled)" : "");
            boolean success = !"CANCELLED".equals(done.status());
            return new TableRunResult(table, cfg, done.sourceCount(), done.destCount(), done.diffCount(), success,
                    success ? null : "Cancelled by user", done.totalEstimate());
        } catch (Exception ex) {
            long elapsed = System.currentTimeMillis() - startMs;
            log.warn("Batch table error [{}] after {} ms: {}", table, elapsed, ex.getMessage());
            // Use whatever live progress CompareEngine last reported (via its
            // periodic checkpoint) instead of hardcoding 0 — this table may have
            // scanned a substantial number of rows before hitting the error.
            // Fall back to the carried-forward resume baseline (not 0) if the
            // engine threw before ever reaching its first checkpoint — otherwise
            // a resumed table that fails again immediately would lose the
            // progress from its PRIOR run too.
            JobStatus lastKnown = liveStatus.get();
            long src = Math.max(lastKnown != null ? lastKnown.sourceCount() : 0, baseSrc);
            long dest = Math.max(lastKnown != null ? lastKnown.destCount() : 0, baseDest);
            long diff = Math.max(lastKnown != null ? lastKnown.diffCount() : 0, baseDiff);
            long totalEstimate = lastKnown != null ? lastKnown.totalEstimate() : 0;
            return new TableRunResult(table, cfg, src, dest, diff, false, ex.getMessage(), totalEstimate);
        }
    }

    /**
     * Runs a set of table configs in parallel (virtual threads, unbounded —
     * cheap enough that "8 max" isn't actually needed) and, unlike a naive
     * "update status only when a whole table finishes" loop, aggregates
     * *live* progress across all currently-running tables every ~500ms.
     *
     * <p>Each table's {@link CompareEngine} already reports its own progress
     * every 1,000 rows into a private {@link AtomicReference}; this method
     * sums all of those in-flight references (running + already-finished)
     * on a short poll loop and republishes the combined total into the
     * shared {@link #status}, so the UI sees smooth incremental progress
     * across the whole batch instead of stepped jumps only at table boundaries.
     */
    private BatchResult runTablesInParallel(List<JobConfig> configs,
                                            Map<JobConfig, Long> jobTableIds,
                                            String jobTs,
                                            List<String> skipped) throws Exception {
        return runTablesInParallel(configs, jobTableIds, jobTs, skipped, Map.of(), null, Map.of(), new AtomicReference<>(JobStatus.running()));
    }

    private BatchResult runTablesInParallel(List<JobConfig> configs,
                                            Map<JobConfig, Long> jobTableIds,
                                            String jobTs,
                                            List<String> skipped,
                                            Map<JobConfig, String> resumeCheckpoints,
                                            AtomicBoolean cancelFlag) throws Exception {
        return runTablesInParallel(configs, jobTableIds, jobTs, skipped, resumeCheckpoints, cancelFlag, Map.of(), new AtomicReference<>(JobStatus.running()));
    }

    private BatchResult runTablesInParallel(List<JobConfig> configs,
                                            Map<JobConfig, Long> jobTableIds,
                                            String jobTs,
                                            List<String> skipped,
                                            Map<JobConfig, String> resumeCheckpoints,
                                            AtomicBoolean cancelFlag,
                                            Map<JobConfig, long[]> resumeBaseCounts) throws Exception {
        return runTablesInParallel(configs, jobTableIds, jobTs, skipped, resumeCheckpoints, cancelFlag, resumeBaseCounts, new AtomicReference<>(JobStatus.running()));
    }

    /**
     * Fully-parameterized variant. {@code jobLevelStatus} is the job's own
     * {@link AtomicReference} (stored in {@link #activeJobStatuses}) — the
     * progress loop writes the aggregated table counts into it so the UI can
     * poll per-job progress independently of any other concurrent job.
     */
    private BatchResult runTablesInParallel(List<JobConfig> configs,
                                            Map<JobConfig, Long> jobTableIds,
                                            String jobTs,
                                            List<String> skipped,
                                            Map<JobConfig, String> resumeCheckpoints,
                                            AtomicBoolean cancelFlag,
                                            Map<JobConfig, long[]> resumeBaseCounts,
                                            AtomicReference<JobStatus> jobLevelStatus) throws Exception {
        log.info("Starting parallel reconciliation with {} table(s)", configs.size());

        Map<JobConfig, AtomicReference<JobStatus>> liveStatuses = new ConcurrentHashMap<>();
        for (JobConfig cfg : configs) {
            long[] base = resumeBaseCounts.get(cfg);
            AtomicReference<JobStatus> ref = base != null
                    ? new AtomicReference<>(new JobStatus("RUNNING", base[0], base[1], base[2], null, 0))
                    : new AtomicReference<>(JobStatus.running());
            liveStatuses.put(cfg, ref);
            Long jtId = jobTableIds.get(cfg);
            if (jtId != null) liveTableStatuses.put(jtId, ref);
        }

        long totalSrc = 0, totalDest = 0, totalDiff = 0;

        try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
            var completion = new ExecutorCompletionService<TableRunResult>(exec);
            for (JobConfig cfg : configs) {
                completion.submit(() -> {
                    Long jtId = jobTableIds.get(cfg);
                    if (jtId != null) jobRepo.markTableRunning(jtId);
                    String resumeAfterPk = resumeCheckpoints.get(cfg);
                    long[] baseCounts = resumeBaseCounts.get(cfg);
                    return runSingleTable(cfg, jobTs, liveStatuses.get(cfg), resumeAfterPk, jtId, cancelFlag, baseCounts);
                });
            }

            int remaining = configs.size();
            while (remaining > 0) {
                Future<TableRunResult> f = completion.poll(500, TimeUnit.MILLISECONDS);

                if (f == null) {
                    // Publish aggregated live progress into this job's own status ref.
                    jobLevelStatus.set(new JobStatus("RUNNING",
                            sumField(liveStatuses, JobStatus::sourceCount),
                            sumField(liveStatuses, JobStatus::destCount),
                            sumField(liveStatuses, JobStatus::diffCount), null,
                            sumField(liveStatuses, JobStatus::totalEstimate)));
                    continue;
                }

                remaining--;
                TableRunResult r = f.get();
                Long jtId = jobTableIds.get(r.config());
                totalSrc  += r.sourceCount();
                totalDest += r.destCount();
                totalDiff += r.diffCount();
                String fullKey = r.config().sourceTable().trim().toUpperCase(Locale.ROOT);
                String tailKey = tailName(r.config().sourceTable()).toUpperCase(Locale.ROOT);
                long[] counts = new long[]{r.sourceCount(), r.destCount()};
                tableRowCounts.put(fullKey, counts);
                tableRowCounts.put(tailKey, counts);
                if (r.success()) {
                    lastConfig.set(r.config());
                    if (jtId != null) jobRepo.completeTable(jtId, "COMPLETED", r.sourceCount(), r.destCount(), r.diffCount(), null);
                    log.info("Batch table complete [{}]: src={} dest={} diff={}",
                            r.tableName(), r.sourceCount(), r.destCount(), r.diffCount());
                } else {
                    skipped.add(r.tableName() + " (" + r.error() + ")");
                    if (jtId != null) jobRepo.completeTable(jtId, "FAILED", r.sourceCount(), r.destCount(), r.diffCount(), r.error());
                    log.warn("Batch table failed [{}]: {}", r.tableName(), r.error());
                }
                AtomicReference<JobStatus> finalRef = new AtomicReference<>(
                        new JobStatus(r.success() ? "COMPLETED" : "FAILED",
                                r.sourceCount(), r.destCount(), r.diffCount(), r.error(), r.totalEstimate()));
                liveStatuses.put(r.config(), finalRef);
                if (jtId != null) liveTableStatuses.put(jtId, finalRef);

                jobLevelStatus.set(new JobStatus("RUNNING",
                        sumField(liveStatuses, JobStatus::sourceCount),
                        sumField(liveStatuses, JobStatus::destCount),
                        sumField(liveStatuses, JobStatus::diffCount), null,
                        sumField(liveStatuses, JobStatus::totalEstimate)));
            }
        }

        return new BatchResult(totalSrc, totalDest, totalDiff);
    }

    private long sumField(Map<JobConfig, AtomicReference<JobStatus>> liveStatuses,
                          java.util.function.ToLongFunction<JobStatus> field) {
        long sum = 0;
        for (AtomicReference<JobStatus> ref : liveStatuses.values()) {
            sum += field.applyAsLong(ref.get());
        }
        return sum;
    }

    private record BatchResult(long totalSrc, long totalDest, long totalDiff) {}

    /** Serializes a JobConfig to JSON for persistence (survives restarts). Returns null on failure. */
    private String toJson(JobConfig cfg) {
        try {
            return objectMapper.writeValueAsString(cfg);
        } catch (Exception e) {
            log.warn("Failed to serialize JobConfig for persistence: {}", e.getMessage());
            return null;
        }
    }

    /** Deserializes a JobConfig previously persisted via {@link #toJson(JobConfig)}. Returns null on failure. */
    private JobConfig fromJson(String json) {
        if (json == null || json.isBlank()) return null;
        try {
            return objectMapper.readValue(json, JobConfig.class);
        } catch (Exception e) {
            log.warn("Failed to deserialize persisted JobConfig: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Auto-resume: called once at startup (after {@link JobRepository#recoverStaleRunningJobs()}
     * has already marked interrupted rows FAILED). Finds every table whose config
     * survived and was interrupted by the restart, then re-runs all of them —
     * ordered oldest job first (so if there were 10 stuck jobs, job #1's tables are
     * submitted before job #10's), but since virtual threads are cheap, they all
     * actually execute concurrently rather than strictly one-at-a-time.
     */
    private void autoResumeInterruptedTables() {
        List<JobRepository.ResumableTable> resumable = jobRepo.findResumableStaleTables();
        if (resumable.isEmpty()) return;

        jobRepo.markStaleRowsResumed(resumable.stream().map(JobRepository.ResumableTable::jobTableId).toList());

        List<JobConfig> configs = new ArrayList<>();
        Map<JobConfig, String> resumeCheckpoints = new ConcurrentHashMap<>();
        Map<JobConfig, long[]> resumeBaseCounts = new ConcurrentHashMap<>();
        for (JobRepository.ResumableTable rt : resumable) {
            JobConfig cfg = fromJson(rt.configJson());
            if (cfg == null) continue;
            configs.add(cfg);
            if (rt.lastPkCheckpoint() != null && !rt.lastPkCheckpoint().isBlank()) {
                resumeCheckpoints.put(cfg, rt.lastPkCheckpoint());
                resumeBaseCounts.put(cfg, new long[]{rt.sourceCount(), rt.destCount(), rt.diffCount()});
            }
        }
        if (configs.isEmpty()) {
            log.warn("Found {} interrupted table row(s) but none had a resumable config_json — skipping auto-resume.",
                    resumable.size());
            return;
        }

        long withCheckpoint = resumeCheckpoints.size();
        log.info("Auto-resuming {} table(s) interrupted by the previous app restart " +
                "({} resuming from a saved checkpoint, {} restarting from the beginning)...",
                configs.size(), withCheckpoint, configs.size() - withCheckpoint);

        for (JobConfig cfg : configs) {
            tableConfigs.put(tailName(cfg.sourceTable()), cfg);
            tableConfigs.put(cfg.sourceTable().trim().toUpperCase(Locale.ROOT), cfg);
        }

        String jobTs = java.time.Instant.now().toString();
        long jobId = jobRepo.createJob(jobTs, configs.size());
        latestStartedJobId.set(jobId);

        AtomicReference<JobStatus> jobStatus = new AtomicReference<>(JobStatus.running());
        activeJobStatuses.put(jobId, jobStatus);
        AtomicBoolean cancelFlag = new AtomicBoolean(false);
        cancelFlags.put(jobId, cancelFlag);

        Map<JobConfig, Long> jobTableIds = new ConcurrentHashMap<>();
        for (JobConfig cfg : configs) {
            String tblName = cfg.sourceTable().trim().toUpperCase(Locale.ROOT);
            long jtId = jobRepo.createJobTable(jobId, jobTs, tblName, toJson(cfg));
            String checkpoint = resumeCheckpoints.get(cfg);
            long[] baseCounts = resumeBaseCounts.get(cfg);
            if (checkpoint != null) {
                long[] base = baseCounts != null ? baseCounts : new long[]{0, 0, 0};
                jobRepo.updateCheckpoint(jtId, checkpoint, base[0], base[1], base[2], 0);
            }
            jobTableIds.put(cfg, jtId);
        }

        Thread.ofVirtual().name("reconciler-auto-resume-" + jobId).start(() -> {
            List<String> skipped = new ArrayList<>();
            try {
                BatchResult result = runTablesInParallel(configs, jobTableIds, jobTs, skipped,
                        resumeCheckpoints, cancelFlag, resumeBaseCounts, jobStatus);
                boolean cancelledMidway = cancelFlag.get();
                String message = "Auto-resumed after app restart" +
                        (skipped.isEmpty() ? "" : "; skipped " + skipped.size() + " table(s): " + String.join("; ", skipped));
                String finalStatus = cancelledMidway ? "FAILED" : "COMPLETED";
                if (cancelledMidway) message = "Cancelled by user; " + message;
                JobStatus finalJobStatus = new JobStatus(finalStatus, result.totalSrc(), result.totalDest(), result.totalDiff(), message);
                jobStatus.set(finalJobStatus);
                lastCompletedStatus.set(finalJobStatus);
                jobRepo.updateJob(jobId, finalStatus, result.totalSrc(), result.totalDest(), result.totalDiff(), message);
                log.info("Auto-resume job #{} complete. src={} dest={} diff={} skipped={} cancelled={}",
                        jobId, result.totalSrc(), result.totalDest(), result.totalDiff(), skipped.size(), cancelledMidway);
            } catch (Exception ex) {
                log.error("Auto-resume job #{} failed", jobId, ex);
                long[] sums = jobRepo.sumJobTableCounts(jobId);
                JobStatus failed = new JobStatus("FAILED", sums[0], sums[1], sums[2], ex.getMessage());
                jobStatus.set(failed);
                lastCompletedStatus.set(failed);
                jobRepo.updateJob(jobId, "FAILED", sums[0], sums[1], sums[2], ex.getMessage());
            } finally {
                cancelFlags.remove(jobId);
                activeJobStatuses.remove(jobId);
            }
        });
    }

    private record TableRunResult(String tableName,
                                  JobConfig config,
                                  long sourceCount,
                                  long destCount,
                                  long diffCount,
                                  boolean success,
                                  String error,
                                  long totalEstimate) {
    }

    private record ResolvedTable(String schemaId,
                                 String tableName,
                                 String sourceQualified,
                                 String destQualified,
                                 String destFilterColumn,
                                 String destFilterValue) {
        String scopedKey() {
            return schemaId.toUpperCase(Locale.ROOT) + "." + tableName.toUpperCase(Locale.ROOT);
        }
    }
}

