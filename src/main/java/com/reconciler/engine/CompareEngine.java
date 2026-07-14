package com.reconciler.engine;

import com.reconciler.config.AppProperties;
import com.reconciler.config.JobConfig;
import com.reconciler.model.JobStatus;
import com.reconciler.repository.DiffRepository;
import com.reconciler.repository.DiffRepository.DiffRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Core Sort-Merge Zipper Engine.
 * Opened per-job by {@link com.reconciler.service.ReconcilerService}.
 */
public class CompareEngine {

    private static final Logger log = LoggerFactory.getLogger(CompareEngine.class);
    private static final int BATCH_FLUSH = 5_000;
    /** How often (in total processed rows) to persist a resume checkpoint. */
    private static final int CHECKPOINT_EVERY = 1_000;

    // Note (R5): diffCount counts each diff *encountered* by the sort-merge
    // zipper. Because both input streams are strictly ordered and de-duplicated
    // by primary key, a given pk_composite can be counted at most once per run,
    // so diffCount matches the number of distinct diff rows produced by THIS
    // run. The DiffRepository unique index (job_ts, table_name, pk_composite)
    // only dedupes ACROSS runs (different job_ts on resume), which is intended —
    // it never causes this counter to exceed the rows persisted for this job_ts.

    /**
     * Invoked periodically (every {@value #CHECKPOINT_EVERY} processed rows) and
     * once more on cancellation, with the last fully-processed {@code pk_composite}
     * PLUS the live progress counts at that moment.
     *
     * <p>Carrying the counts (not just the PK) lets the caller persist them into
     * {@code job_tables}, so that if the app crashes/restarts before this table
     * finishes, the row that {@code JobRepository.recoverStaleRunningJobs()}
     * marks FAILED shows how far the scan actually got instead of 0/0/0 — those
     * columns were previously only ever written once, at successful completion.
     */
    @FunctionalInterface
    public interface CheckpointCallback {
        void onCheckpoint(String lastPk, long sourceCount, long destCount, long diffCount, long totalEstimate);
    }

    private final JobConfig              config;
    private final AppProperties          props;
    private final DiffRepository         diffRepo;
    private final AtomicReference<JobStatus> statusRef;
    private final String                 jobTimestamp;
    private final String                 resumeAfterPk;
    private final CheckpointCallback     checkpointCallback;
    private final AtomicBoolean          cancelled;
    /** Counts already accumulated by a PRIOR (interrupted) run of this same
     *  table before a checkpoint-resume — carried forward as the starting
     *  point so the final counts reflect the whole table, not just the rows
     *  processed since the restart. Zero for a normal (non-resumed) run. */
    private final long                   baseSourceCount;
    private final long                   baseDestCount;
    private final long                   baseDiffCount;

    public CompareEngine(JobConfig config,
                         AppProperties props,
                         DiffRepository diffRepo,
                         AtomicReference<JobStatus> statusRef) {
        this(config, props, diffRepo, statusRef, null);
    }

    public CompareEngine(JobConfig config,
                         AppProperties props,
                         DiffRepository diffRepo,
                         AtomicReference<JobStatus> statusRef,
                         String jobTs) {
        this(config, props, diffRepo, statusRef, jobTs, null, null);
    }

    /**
     * Full constructor supporting crash-resume.
     *
     * @param resumeAfterPk      if non-null, both source and dest queries are
     *                           filtered to {@code pk_composite > resumeAfterPk},
     *                           so an interrupted scan restarts past the last
     *                           checkpointed key instead of re-scanning everything.
     * @param checkpointCallback if non-null, invoked periodically (every
     *                           {@value #CHECKPOINT_EVERY} processed rows) with
     *                           the last fully-processed {@code pk_composite},
     *                           so the caller can persist it for future resumes.
     */
    public CompareEngine(JobConfig config,
                         AppProperties props,
                         DiffRepository diffRepo,
                         AtomicReference<JobStatus> statusRef,
                         String jobTs,
                         String resumeAfterPk,
                         CheckpointCallback checkpointCallback) {
        this(config, props, diffRepo, statusRef, jobTs, resumeAfterPk, checkpointCallback, null);
    }

    /**
     * Fullest constructor: also accepts a shared {@code cancelled} flag so an
     * in-flight scan can be stopped promptly (e.g. when its job is deleted from
     * the UI while still RUNNING) instead of running to completion regardless.
     * Checked every loop iteration — cancellation takes effect within a single
     * row's CPU processing time once rows are flowing. Note it is only observed
     * <em>between</em> rows: if the iterator is blocked inside a slow Snowflake
     * chunk download or Oracle page fetch, cancellation won't be seen until that
     * blocking network fetch returns the next row (R3).
     */
    public CompareEngine(JobConfig config,
                         AppProperties props,
                         DiffRepository diffRepo,
                         AtomicReference<JobStatus> statusRef,
                         String jobTs,
                         String resumeAfterPk,
                         CheckpointCallback checkpointCallback,
                         AtomicBoolean cancelled) {
        this(config, props, diffRepo, statusRef, jobTs, resumeAfterPk, checkpointCallback, cancelled, 0, 0, 0);
    }

    /**
     * Fullest constructor of all: also accepts a baseline source/dest/diff
     * count carried forward from a PRIOR interrupted run of this same table
     * (see {@link com.reconciler.repository.JobRepository.ResumableTable}).
     * Without this, a resumed scan's final counts only reflect the rows
     * processed AFTER the restart — silently losing whatever progress was
     * made before the crash from every count/percent/history view.
     */
    public CompareEngine(JobConfig config,
                         AppProperties props,
                         DiffRepository diffRepo,
                         AtomicReference<JobStatus> statusRef,
                         String jobTs,
                         String resumeAfterPk,
                         CheckpointCallback checkpointCallback,
                         AtomicBoolean cancelled,
                         long baseSourceCount,
                         long baseDestCount,
                         long baseDiffCount) {
        this.config        = config;
        this.props         = props;
        this.diffRepo      = diffRepo;
        this.statusRef     = statusRef;
        this.jobTimestamp  = jobTs != null ? jobTs : Instant.now().toString();
        this.resumeAfterPk = resumeAfterPk;
        this.checkpointCallback = checkpointCallback;
        this.cancelled     = cancelled;
        this.baseSourceCount = baseSourceCount;
        this.baseDestCount   = baseDestCount;
        this.baseDiffCount   = baseDiffCount;
    }

    /**
     * Execute the full sort-merge comparison.
     * Blocks until both iterators are exhausted.
     */
    public void run() throws Exception {

        long srcCount  = baseSourceCount;
        long destCount = baseDestCount;
        long diffCount = baseDiffCount;
        long totalEstimate = 0;
        String tableName = qualifiedName(config.sourceTable());
        List<DiffRecord> batch = new ArrayList<>(BATCH_FLUSH);
        String lastProcessedPk = resumeAfterPk;

        if (resumeAfterPk != null) {
            log.info("Resuming {} from checkpoint pk_composite > '{}'", tableName, resumeAfterPk);
        }

        try (Connection srcConn = openSource();
             Connection dstConn = openDest()) {

            QueryStrategy srcStrategy = new OracleQueryStrategy();
            QueryStrategy dstStrategy = new SnowflakeQueryStrategy();

            List<QueryStrategy.ColumnInfo> srcCols = loadColumnInfo(srcConn, config.sourceTable());
            List<QueryStrategy.ColumnInfo> dstCols = loadColumnInfo(dstConn, config.destTable());

            // Best-effort total row estimate (COUNT(*) on both sides) so the UI can show
            // a real "% complete" instead of just a running counter with no denominator.
            // Failures here are non-fatal — we just fall back to "unknown" percent.
            totalEstimate = estimateTotalRows(srcConn, dstConn, tableName);

            String srcSql = srcStrategy.buildHashQuery(config, srcCols, resumeAfterPk);
            String dstSql = dstStrategy.buildHashQuery(config, dstCols, resumeAfterPk);

            log.info("Source query:\n{}", srcSql);
            log.info("Dest query:\n{}", dstSql);

            // ── Open iterators using pre-built SQL ──
            try (OracleBatchIterator srcIter  = new OracleBatchIterator(srcConn, config, resumeAfterPk);
                 SnowflakeRowIterator destIter = SnowflakeRowIterator.open(dstConn, config, props, dstCols, resumeAfterPk)) {

            Row srcRow  = srcIter.hasNext()  ? srcIter.next()  : null;
            Row destRow = destIter.hasNext() ? destIter.next() : null;
            boolean wasCancelled = false;

            while (srcRow != null || destRow != null) {

                if (cancelled != null && cancelled.get()) {
                    wasCancelled = true;
                    log.warn("Job cancelled — stopping scan of {} early (src={} dest={} diffs={} so far)",
                            tableName, srcCount, destCount, diffCount);
                    break;
                }

                if (srcRow != null && destRow != null) {
                    int cmp = comparePk(srcRow.pkComposite(), destRow.pkComposite());

                    if (cmp == 0) {
                        // ── Keys match — compare hashes ──────────────
                        srcCount++;
                        destCount++;
                        if (!srcRow.rowHash().equalsIgnoreCase(destRow.rowHash())) {
                            diffCount++;
                            String changedCols = detectChangedColumns(srcConn, dstConn, srcRow.pkComposite(), srcCols, dstCols);
                            batch.add(new DiffRecord(tableName, srcRow.pkComposite(), "HASH_MISMATCH",
                                    srcRow.rowHash(), destRow.rowHash(), changedCols));
                        }
                        lastProcessedPk = srcRow.pkComposite();
                        srcRow  = srcIter.hasNext()  ? srcIter.next()  : null;
                        destRow = destIter.hasNext() ? destIter.next() : null;

                    } else if (cmp < 0) {
                        // ── Source PK is smaller → missing in dest ───
                        srcCount++;
                        diffCount++;
                        batch.add(new DiffRecord(tableName, srcRow.pkComposite(), "MISSING_IN_DEST", srcRow.rowHash(), null, null));
                        lastProcessedPk = srcRow.pkComposite();
                        srcRow = srcIter.hasNext() ? srcIter.next() : null;

                    } else {
                        // ── Dest PK is smaller → missing in source ───
                        destCount++;
                        diffCount++;
                        batch.add(new DiffRecord(tableName, destRow.pkComposite(), "MISSING_IN_SOURCE", null, destRow.rowHash(), null));
                        lastProcessedPk = destRow.pkComposite();
                        destRow = destIter.hasNext() ? destIter.next() : null;
                    }

                } else if (srcRow != null) {
                    srcCount++;
                    diffCount++;
                    batch.add(new DiffRecord(tableName, srcRow.pkComposite(), "MISSING_IN_DEST", srcRow.rowHash(), null, null));
                    lastProcessedPk = srcRow.pkComposite();
                    srcRow = srcIter.hasNext() ? srcIter.next() : null;

                } else {
                    destCount++;
                    diffCount++;
                    batch.add(new DiffRecord(tableName, destRow.pkComposite(), "MISSING_IN_SOURCE", null, destRow.rowHash(), null));
                    lastProcessedPk = destRow.pkComposite();
                    destRow = destIter.hasNext() ? destIter.next() : null;
                }

                // Flush diff batch to SQLite periodically (size-based safety valve —
                // the checkpoint-based flush below usually fires first for most jobs).
                if (batch.size() >= BATCH_FLUSH) {
                    diffRepo.insertBatch(jobTimestamp, batch);
                    batch.clear();
                }

                // Update live status + resume checkpoint every 1000 processed rows.
                // Also flush any pending diffs here (not just at BATCH_FLUSH=5000) so the
                // Difference Log in the UI shows new rows progressively as a table scans,
                // instead of only appearing once the whole table finishes.
                long total = srcCount + destCount;
                if (total % CHECKPOINT_EVERY == 0) {
                    if (!batch.isEmpty()) {
                        diffRepo.insertBatch(jobTimestamp, batch);
                        batch.clear();
                    }
                    JobStatus progress = new JobStatus("RUNNING", srcCount, destCount, diffCount, null, totalEstimate);
                    statusRef.set(progress);
                    int pct = progress.percentComplete();
                    log.info("Progress — src: {} | dest: {} | diffs: {}{}", srcCount, destCount, diffCount,
                            pct >= 0 ? " (" + pct + "% complete)" : "");
                    if (checkpointCallback != null && lastProcessedPk != null) {
                        checkpointCallback.onCheckpoint(lastProcessedPk, srcCount, destCount, diffCount, totalEstimate);
                    }
                }
            }

            // Flush any remaining diffs
            if (!batch.isEmpty()) {
                diffRepo.insertBatch(jobTimestamp, batch);
            }

            if (wasCancelled) {
                if (checkpointCallback != null && lastProcessedPk != null) {
                    checkpointCallback.onCheckpoint(lastProcessedPk, srcCount, destCount, diffCount, totalEstimate);
                }
                statusRef.set(new JobStatus("CANCELLED", srcCount, destCount, diffCount, "Cancelled by user", totalEstimate));
                log.warn("🛑 Job cancelled — src: {} | dest: {} | diffs: {}", srcCount, destCount, diffCount);
                return;
            }
            }
        }

        statusRef.set(new JobStatus("COMPLETED", srcCount, destCount, diffCount, null, totalEstimate));
        log.info("✅ Job complete — src: {} | dest: {} | diffs: {}", srcCount, destCount, diffCount);
    }

    /**
     * Compares two {@code pk_composite} strings by their raw UTF-8 bytes
     * (CODE_REVIEW C1). Both Oracle (forced {@code NLS_SORT=BINARY} in
     * {@link #openSource()}) and Snowflake (default UTF-8) order their result
     * sets by binary byte value, so the merge zipper MUST use that exact same
     * ordering — {@code String.compareTo} (UTF-16 code units) diverges for
     * supplementary (astral) characters and would desynchronize the streams.
     */
    private static int comparePk(String a, String b) {
        byte[] ba = a.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] bb = b.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int len = Math.min(ba.length, bb.length);
        for (int i = 0; i < len; i++) {
            int ca = ba[i] & 0xFF; // unsigned byte comparison
            int cb = bb[i] & 0xFF;
            if (ca != cb) return Integer.compare(ca, cb);
        }
        return Integer.compare(ba.length, bb.length);
    }

    /**
     * Best-effort {@code COUNT(*)} on both source and dest tables (honoring the
     * dest filter / modified-column buffer where configured, so the estimate is
     * reasonably close to what the scan will actually iterate). Returns 0 if
     * either count fails, meaning "unknown" — callers must treat 0 as no-percent-available.
     */
    private long estimateTotalRows(Connection srcConn, Connection dstConn, String tableName) {
        try {
            long srcTotal = countRows(srcConn, config.sourceTable(), true);
            long dstTotal = countRows(dstConn, config.destTable(), false);
            return srcTotal + dstTotal;
        } catch (Exception e) {
            log.warn("Could not estimate total row count for {} — progress % will be unavailable: {}", tableName, e.getMessage());
            return 0;
        }
    }

    private long countRows(Connection conn, String table, boolean oracle) throws SQLException {
        // Validate the identifier before concatenating into SQL (S2). The dest
        // filter VALUE is bound with a ? param (never inlined).
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM ")
                .append(QueryStrategy.requireSafeIdentifier(table));
        List<String> predicates = new ArrayList<>();
        if (!oracle && config.hasDestFilter()) {
            predicates.add(QueryStrategy.requireSafeIdentifier(config.destFilterColumn()) + " = ?");
        }
        if (!predicates.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", predicates));
        }
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            if (!oracle && config.hasDestFilter()) {
                ps.setString(1, config.destFilterValue());
            }
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0;
            }
        }
    }

    private String tailName(String qualified) {
        String safe = qualified == null ? "UNKNOWN_TABLE" : qualified;
        int dot = safe.lastIndexOf('.');
        return (dot >= 0 ? safe.substring(dot + 1) : safe).toUpperCase(Locale.ROOT);
    }

    private String qualifiedName(String tableRef) {
        if (tableRef == null || tableRef.isBlank()) {
            return "UNKNOWN_TABLE";
        }
        return tableRef.trim().toUpperCase(Locale.ROOT);
    }

    // ── Connection helpers ───────────────────────────────────────────────

    private Connection openSource() throws SQLException {
        Connection c = DriverManager.getConnection(
                props.oracle().jdbcUrl(),
                props.oracle().user(),
                props.oracle().password());
        c.setAutoCommit(false); // required for Oracle streaming / keyset queries
        // ── CODE_REVIEW C1: force BINARY collation on the source session ──
        // The sort-merge zipper compares pk_composite in Java by raw UTF-8 bytes
        // (see run()). Oracle's ORDER BY must therefore use the SAME binary byte
        // ordering, not an NLS linguistic/case-insensitive sort — otherwise the
        // two streams desynchronize and produce bogus MISSING_IN_* diffs (or
        // silently skip real ones). READ ONLY also makes the long-running scan's
        // read-consistency intent explicit and reduces undo pressure (R2).
        try (Statement s = c.createStatement()) {
            s.execute("ALTER SESSION SET NLS_SORT = BINARY");
            s.execute("ALTER SESSION SET NLS_COMP = BINARY");
        }
        try {
            c.setReadOnly(true);
        } catch (SQLException ignored) {
            // Best-effort; not all drivers honor read-only on an active txn.
        }
        return c;
    }

    private Connection openDest() throws SQLException {
        return DriverManager.getConnection(
                props.snowflake().jdbcUrl(),
                props.snowflake().user(),
                props.snowflake().password());
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

    /**
     * Column-level diff for one PK. Called only for HASH_MISMATCH rows.
     *
     * @return comma-separated changed column names, e.g. "AMOUNT,STATUS"
     */
    private String detectChangedColumns(Connection srcConn,
                                        Connection dstConn,
                                        String pkComposite,
                                        List<QueryStrategy.ColumnInfo> srcCols,
                                        List<QueryStrategy.ColumnInfo> dstCols) {
        try {
            List<String> keyCols = config.keyColumns();
            String[] pkVals = pkComposite.split("~", -1);
            if (pkVals.length != keyCols.size()) return "";

            // For UI diagnostics we compare hashed columns + modified column (if configured),
            // so users can see CDC timestamp drift even when it is excluded from hashing.
            LinkedHashSet<String> compareSet = new LinkedHashSet<>();
            if (config.hashColumns() != null && !config.hashColumns().isEmpty()) compareSet.addAll(config.hashColumns());
            else compareSet.addAll(keyCols);
            if (config.modifiedColumn() != null && !config.modifiedColumn().isBlank()) {
                compareSet.add(config.modifiedColumn().trim());
            }
            List<String> compareCols = new ArrayList<>(compareSet);

            Map<String, String> srcMap = fetchRowAsStrings(srcConn, config.sourceTable(), keyCols, pkVals, compareCols, true, null, null);
            Map<String, String> dstMap = fetchRowAsStrings(dstConn, config.destTable(), keyCols, pkVals, compareCols, false,
                    config.destFilterColumn(), config.destFilterValue());

            List<String> changed = new ArrayList<>();
            for (String col : compareCols) {
                String key = col.toUpperCase(Locale.ROOT);
                String srcType = srcCols.stream().filter(c -> c.name().equalsIgnoreCase(col)).map(QueryStrategy.ColumnInfo::dataType).findFirst().orElse("");
                String dstType = dstCols.stream().filter(c -> c.name().equalsIgnoreCase(col)).map(QueryStrategy.ColumnInfo::dataType).findFirst().orElse("");
                String type = !srcType.isBlank() ? srcType : dstType;

                String a = ValueNormalizer.normalize(srcMap.get(key), type);
                String b = ValueNormalizer.normalize(dstMap.get(key), type);
                if (!Objects.equals(a, b)) changed.add(col);
            }
            return String.join(",", changed);
        } catch (Exception e) {
            log.warn("Could not detect changed columns for pk {}: {}", pkComposite, e.getMessage());
            return "";
        }
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

        String sql = "SELECT " + selectCols + " FROM " + table + " WHERE " + where + (oracle ? " FETCH FIRST 1 ROWS ONLY" : " LIMIT 1");

        Map<String, String> out = new HashMap<>();
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
                        out.put(c.toUpperCase(Locale.ROOT), rs.getString(c));
                    }
                }
            }
        }
        return out;
    }

    private String safeIdentifier(String ident) {
        String upper = ident == null ? "" : ident.trim().toUpperCase(Locale.ROOT);
        if (!upper.matches("[A-Z0-9_\\$]+")) {
            throw new IllegalArgumentException("Unsupported identifier format: " + ident);
        }
        return upper;
    }
}
