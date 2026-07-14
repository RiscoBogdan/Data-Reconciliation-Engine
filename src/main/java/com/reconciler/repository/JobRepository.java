package com.reconciler.repository;

import com.reconciler.config.AppProperties;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.util.*;

/**
 * Persists job run history and per-table status into SQLite.
 * Tables: {@code jobs}, {@code job_tables}.
 */
@Repository
public class JobRepository {

    private final String jdbcUrl;

    public JobRepository(AppProperties props) {
        this.jdbcUrl = "jdbc:sqlite:" + props.sqlite().path();
        initialise();
    }

    public synchronized void initialise() {
        try (Connection c = conn(); Statement s = c.createStatement()) {
            s.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_ts       TEXT    NOT NULL UNIQUE,
                    status       TEXT    NOT NULL DEFAULT 'RUNNING',
                    table_count  INTEGER NOT NULL DEFAULT 0,
                    source_count INTEGER NOT NULL DEFAULT 0,
                    dest_count   INTEGER NOT NULL DEFAULT 0,
                    diff_count   INTEGER NOT NULL DEFAULT 0,
                    error_msg    TEXT,
                    started_at   TEXT    NOT NULL DEFAULT '',
                    finished_at  TEXT
                )
            """);

            s.execute("""
                CREATE TABLE IF NOT EXISTS job_tables (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id       INTEGER NOT NULL REFERENCES jobs(id),
                    job_ts       TEXT    NOT NULL,
                    table_name   TEXT    NOT NULL,
                    status       TEXT    NOT NULL DEFAULT 'PENDING',
                    source_count INTEGER NOT NULL DEFAULT 0,
                    dest_count   INTEGER NOT NULL DEFAULT 0,
                    diff_count   INTEGER NOT NULL DEFAULT 0,
                    error_msg    TEXT,
                    started_at   TEXT,
                    finished_at  TEXT,
                    config_json  TEXT,
                    last_pk_checkpoint TEXT
                )
            """);

            // Older DBs created before config_json/last_pk_checkpoint existed:
            // add them if missing. SQLite has no "ADD COLUMN IF NOT EXISTS",
            // so probe and ignore the duplicate-column error if already there.
            try {
                s.execute("ALTER TABLE job_tables ADD COLUMN config_json TEXT");
            } catch (SQLException ignore) {
                // column already exists — fine.
            }
            try {
                s.execute("ALTER TABLE job_tables ADD COLUMN last_pk_checkpoint TEXT");
            } catch (SQLException ignore) {
                // column already exists — fine.
            }
            try {
                s.execute("ALTER TABLE job_tables ADD COLUMN total_estimate INTEGER NOT NULL DEFAULT 0");
            } catch (SQLException ignore) {
                // column already exists — fine.
            }

            s.execute("CREATE INDEX IF NOT EXISTS idx_job_tables_job_id ON job_tables(job_id)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_job_tables_job_ts ON job_tables(job_ts)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialise job tables", e);
        }
    }

    // ── Jobs CRUD ─────────────────────────────────────────────────────────

    /** Create a new job row. Returns the generated job ID. */
    public synchronized long createJob(String jobTs, int tableCount) {
        String sql = "INSERT INTO jobs(job_ts, status, table_count, started_at) VALUES(?, 'RUNNING', ?, ?)";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, jobTs);
            ps.setInt(2, tableCount);
            ps.setString(3, jobTs);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) return rs.getLong(1);
                throw new SQLException("No generated key for job insert");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create job", e);
        }
    }

    /** Update the table_count column after the real count is known (used by startBatch). */
    public synchronized void updateJobTableCount(long jobId, int tableCount) {
        String sql = "UPDATE jobs SET table_count = ? WHERE id = ?";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setInt(1, tableCount);
            ps.setLong(2, jobId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update job table count", e);
        }
    }

    /** Update job status and aggregate counts. */
    public synchronized void updateJob(long jobId, String status, long sourceCount, long destCount, long diffCount, String error) {
        String sql = "UPDATE jobs SET status = ?, source_count = ?, dest_count = ?, diff_count = ?, error_msg = ?, finished_at = CASE WHEN ? IN ('COMPLETED','FAILED') THEN datetime('now') ELSE finished_at END WHERE id = ?";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setLong(2, sourceCount);
            ps.setLong(3, destCount);
            ps.setLong(4, diffCount);
            ps.setString(5, error);
            ps.setString(6, status);
            ps.setLong(7, jobId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update job", e);
        }
    }

    /** Sum of source/dest row counts across ALL historical jobs (COMPLETED or FAILED).
     *  Unlike a per-table live cache, this reflects every past run — including
     *  repeated runs of the same table — for the "all history" view. */
    public synchronized long[] sumAllJobRowCounts() {
        String sql = "SELECT COALESCE(SUM(source_count),0), COALESCE(SUM(dest_count),0) FROM jobs WHERE status IN ('COMPLETED','FAILED')";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            if (rs.next()) return new long[]{rs.getLong(1), rs.getLong(2)};
            return new long[]{0L, 0L};
        } catch (SQLException e) {
            throw new RuntimeException("Failed to sum job row counts", e);
        }
    }

    /** Latest known source/dest row counts per table, from the most recent
     *  RUNNING, COMPLETED, **or** FAILED job_tables row for each table. Unlike
     *  the transient in-memory cache in ReconcilerService (which is cleared at
     *  the start of every new run, and is gone entirely after a restart), this
     *  survives across runs of *other* tables — so switching tables/scopes
     *  doesn't lose data.
     *
     *  <p>Includes FAILED rows because a table interrupted by an app restart
     *  still has real, meaningful progress counts persisted at its last
     *  checkpoint (see {@link #updateCheckpoint}) — excluding it here made
     *  "Show all history" show 0/0/0 for any table whose only run so far was
     *  interrupted, even though the FAILED row had accurate partial counts.
     *
     *  <p>Also includes RUNNING rows for the same reason: {@link #updateCheckpoint}
     *  now persists live progress counts every ~1000 rows WHILE a table is still
     *  RUNNING (not just once at the end). Without RUNNING here, a table that's
     *  mid-scan right now would keep showing whatever its PREVIOUS (older, stale)
     *  COMPLETED/FAILED row had — and then once it finishes and the in-memory
     *  cache picks up the fresh number, the total would look like it "jumped"
     *  or "added" on top of the old value instead of just reflecting live progress
     *  the whole time.
     */
    public synchronized Map<String, long[]> latestRowCountsByTable() {
        String sql = """
                SELECT table_name, source_count, dest_count
                FROM job_tables jt
                WHERE status IN ('RUNNING', 'COMPLETED', 'FAILED')
                  AND id = (SELECT MAX(id) FROM job_tables jt2
                            WHERE jt2.table_name = jt.table_name AND jt2.status IN ('RUNNING', 'COMPLETED', 'FAILED'))
                """;
        Map<String, long[]> out = new LinkedHashMap<>();
        try (Connection c = conn(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String tbl = String.valueOf(rs.getString("table_name")).toUpperCase(Locale.ROOT);
                out.put(tbl, new long[]{rs.getLong("source_count"), rs.getLong("dest_count")});
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to fetch latest row counts by table", e);
        }
        return out;
    }

    /** Sum of source/dest row counts per table across EVERY run of that table —
     *  used for the "all history" per-table breakdown so repeated runs accumulate.
     *  Includes RUNNING/FAILED rows for the same reasons as
     *  {@link #latestRowCountsByTable()} above. */
    public synchronized Map<String, long[]> sumRowCountsByTable() {
        String sql = "SELECT table_name, SUM(source_count), SUM(dest_count) FROM job_tables WHERE status IN ('RUNNING', 'COMPLETED', 'FAILED') GROUP BY table_name";
        Map<String, long[]> out = new LinkedHashMap<>();
        try (Connection c = conn(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String tbl = String.valueOf(rs.getString(1)).toUpperCase(Locale.ROOT);
                out.put(tbl, new long[]{rs.getLong(2), rs.getLong(3)});
            }
        } catch (SQLException e) {

            throw new RuntimeException("Failed to sum row counts by table", e);
        }
        return out;
    }

    /** Get a single job by ID. */
    public synchronized Map<String, Object> getJob(long jobId) {
        String sql = "SELECT * FROM jobs WHERE id = ?";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, jobId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return jobFromRow(rs);
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to fetch job", e);
        }
    }

    /** Get a single job by job_ts. */
    public synchronized Map<String, Object> getJobByTs(String jobTs) {
        String sql = "SELECT * FROM jobs WHERE job_ts = ?";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, jobTs);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return jobFromRow(rs);
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to fetch job by ts", e);
        }
    }

    /** Paginated list of jobs, newest first. */
    public synchronized List<Map<String, Object>> listJobs(int page, int size) {
        int offset = (page - 1) * size;
        String sql = "SELECT * FROM jobs ORDER BY id DESC LIMIT ? OFFSET ?";
        List<Map<String, Object>> out = new ArrayList<>();
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setInt(1, size);
            ps.setInt(2, offset);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(jobFromRow(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list jobs", e);
        }
        return out;
    }

    /** Total job count. */
    public synchronized int countJobs() {
        try (Connection c = conn(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM jobs")) {
            return rs.next() ? rs.getInt(1) : 0;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count jobs", e);
        }
    }

    // ── Job Tables CRUD ───────────────────────────────────────────────────

    /** Register a table for a job. Returns the generated job_table ID. */
    public synchronized long createJobTable(long jobId, String jobTs, String tableName) {
        return createJobTable(jobId, jobTs, tableName, null);
    }

    /** Register a table for a job, persisting its JobConfig as JSON so it can
     *  be reconstructed and auto-resumed if the app restarts mid-run. */
    public synchronized long createJobTable(long jobId, String jobTs, String tableName, String configJson) {
        String sql = "INSERT INTO job_tables(job_id, job_ts, table_name, status, config_json) VALUES(?, ?, ?, 'PENDING', ?)";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setLong(1, jobId);
            ps.setString(2, jobTs);
            ps.setString(3, tableName);
            ps.setString(4, configJson);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) return rs.getLong(1);
                throw new SQLException("No generated key for job_table insert");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create job_table", e);
        }
    }

    /** Mark a table as RUNNING. */
    public synchronized void markTableRunning(long jobTableId) {
        String sql = "UPDATE job_tables SET status = 'RUNNING', started_at = datetime('now') WHERE id = ?";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, jobTableId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark table running", e);
        }
    }

    /** Mark a table as COMPLETED or FAILED. */
    public synchronized void completeTable(long jobTableId, String status, long sourceCount, long destCount, long diffCount, String error) {
        String sql = "UPDATE job_tables SET status = ?, source_count = ?, dest_count = ?, diff_count = ?, error_msg = ?, finished_at = datetime('now') WHERE id = ?";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setLong(2, sourceCount);
            ps.setLong(3, destCount);
            ps.setLong(4, diffCount);
            ps.setString(5, error);
            ps.setLong(6, jobTableId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to complete table", e);
        }
    }

    /**
     * Persists the last fully-processed {@code pk_composite} for a running table,
     * so that if the app restarts mid-scan, {@link #findResumableStaleTables()}
     * can hand back this checkpoint and the re-run can pick up from here instead
     * of re-scanning the whole table from the beginning.
     *
     * <p>Also persists the current progress counts and total-row estimate, so
     * that IF the app crashes/restarts before this table finishes, the
     * automatically-created FAILED row (see {@link #recoverStaleRunningJobs()})
     * shows how far the scan actually got (Source/Dest/Diffs + % complete)
     * instead of misleadingly showing 0/0/0 — those columns were previously
     * only ever written once, at successful completion via {@link #completeTable}.
     */
    public synchronized void updateCheckpoint(long jobTableId, String lastPk,
                                              long sourceCount, long destCount, long diffCount, long totalEstimate) {
        String sql = "UPDATE job_tables SET last_pk_checkpoint = ?, source_count = ?, dest_count = ?, " +
                "diff_count = ?, total_estimate = ? WHERE id = ?";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, lastPk);
            ps.setLong(2, sourceCount);
            ps.setLong(3, destCount);
            ps.setLong(4, diffCount);
            ps.setLong(5, totalEstimate);
            ps.setLong(6, jobTableId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update checkpoint", e);
        }
    }

    /**
     * Legacy overload — persists only the checkpoint PK without progress counts.
     * Kept for any callers that don't have live counts handy; prefer the
     * 5-arg overload above whenever possible so crash-recovered FAILED rows
     * show accurate progress instead of 0/0/0.
     */
    public synchronized void updateCheckpoint(long jobTableId, String lastPk) {
        String sql = "UPDATE job_tables SET last_pk_checkpoint = ? WHERE id = ?";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, lastPk);
            ps.setLong(2, jobTableId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update checkpoint", e);
        }
    }

    /** Get all tables for a job. */
    public synchronized List<Map<String, Object>> getJobTables(long jobId) {
        String sql = "SELECT * FROM job_tables WHERE job_id = ? ORDER BY id";
        List<Map<String, Object>> out = new ArrayList<>();
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, jobId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(jobTableFromRow(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to fetch job tables", e);
        }
        return out;
    }

    /** Delete a job and its tables (cascading). */
    public synchronized void deleteJob(long jobId) {
        try (Connection c = conn()) {
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM job_tables WHERE job_id = ?")) {
                ps.setLong(1, jobId);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM jobs WHERE id = ?")) {
                ps.setLong(1, jobId);
                ps.executeUpdate();
            }
            c.commit();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete job", e);
        }
    }

    /** Wipe ALL job history (jobs + job_tables). Used by the "Clear" action so
     *  persisted row-count history doesn't linger after the diffs table is cleared. */
    public synchronized void clearAll() {
        try (Connection c = conn(); Statement s = c.createStatement()) {
            s.execute("DELETE FROM job_tables");
            s.execute("DELETE FROM jobs");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to clear job history", e);
        }
    }

    /**
     * Crash recovery: on app startup, any job/job_table left in RUNNING (or
     * still-queued PENDING) state belongs to a previous process that died
     * (JVM kill, restart, crash) before it could mark itself COMPLETED/FAILED.
     * Since no in-memory work actually survives a restart (see
     * ReconcilerService — status/thread state is gone), these rows would
     * otherwise sit as misleading "RUNNING" forever in Job History even
     * though nothing is actually executing anymore.
     *
     * <p>Before marking them FAILED, this captures each table's persisted
     * {@code config_json} (if present) so the caller can automatically
     * re-run them — see {@link #findResumableStaleTables()}.
     *
     * <p>Any diff rows already flushed to SQLite before the crash are left
     * as-is (they may represent a partial/truncated scan of that table).
     *
     * @return number of stale job/job_table rows recovered (for startup logging).
     */
    public synchronized int recoverStaleRunningJobs() {
        String msg = "Interrupted by application restart (job was RUNNING when the app stopped)";
        String updateTables = "UPDATE job_tables SET status = 'FAILED', error_msg = ?, finished_at = datetime('now') WHERE status IN ('RUNNING','PENDING')";
        // Aggregate source/dest/diff counts from this job's job_tables rows (which
        // were just marked FAILED above, carrying whatever partial progress
        // updateCheckpoint() had persisted) INTO the parent jobs row — otherwise
        // a crash-recovered job stays stuck at 0/0/0 in Job History forever,
        // even though its child tables show real partial counts.
        String updateJobs = "UPDATE jobs SET status = 'FAILED', error_msg = ?, finished_at = datetime('now'), " +
                "source_count = (SELECT COALESCE(SUM(source_count),0) FROM job_tables WHERE job_tables.job_id = jobs.id), " +
                "dest_count = (SELECT COALESCE(SUM(dest_count),0) FROM job_tables WHERE job_tables.job_id = jobs.id), " +
                "diff_count = (SELECT COALESCE(SUM(diff_count),0) FROM job_tables WHERE job_tables.job_id = jobs.id) " +
                "WHERE status = 'RUNNING'";
        try (Connection c = conn()) {
            int tableRows;
            int jobRows;
            try (PreparedStatement ps = c.prepareStatement(updateTables)) {
                ps.setString(1, msg);
                tableRows = ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement(updateJobs)) {
                ps.setString(1, msg);
                jobRows = ps.executeUpdate();
            }
            return jobRows + tableRows;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to recover stale running jobs", e);
        }
    }

    /**
     * Sums source/dest/diff counts across all job_tables rows belonging to a
     * job, regardless of their individual status (RUNNING/COMPLETED/FAILED).
     * Used to populate the parent job's aggregate counts in situations where
     * the job as a whole is being marked FAILED (e.g. an unexpected exception,
     * or "no tables processed") but individual tables may still have made real
     * partial progress that shouldn't be discarded as 0/0/0.
     */
    public synchronized long[] sumJobTableCounts(long jobId) {
        String sql = "SELECT COALESCE(SUM(source_count),0), COALESCE(SUM(dest_count),0), COALESCE(SUM(diff_count),0) " +
                "FROM job_tables WHERE job_id = ?";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, jobId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return new long[]{rs.getLong(1), rs.getLong(2), rs.getLong(3)};
                return new long[]{0L, 0L, 0L};
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to sum job_table counts", e);
        }
    }

    /** One row eligible for auto-resume after a crash/restart.
     *  {@code sourceCount}/{@code destCount}/{@code diffCount} are the counts
     *  already persisted at the last checkpoint BEFORE the crash — callers
     *  must carry these forward as the resumed run's starting baseline (not
     *  discard them), otherwise a resumed scan only reports the rows processed
     *  AFTER the restart instead of the true cumulative total for that table. */
    public record ResumableTable(long jobTableId, long jobId, String jobTs, String tableName, String configJson,
                                 String lastPkCheckpoint, long sourceCount, long destCount, long diffCount) {}

    /**
     * Finds tables whose {@code config_json} was persisted but which never
     * reached COMPLETED — i.e. were interrupted by a restart and just got
     * marked FAILED by {@link #recoverStaleRunningJobs()} (call this AFTER
     * that, using the error message it wrote, so we only pick up rows from
     * *this* recovery pass, not older genuinely-failed runs).
     *
     * <p>Ordered oldest job first (ascending job id) so callers that submit
     * work in this order naturally prioritize the oldest interrupted job,
     * even though — with virtual threads — everything still executes in
     * parallel rather than strictly one-at-a-time.
     *
     * <p><b>Important:</b> callers must follow up with {@link #markStaleRowsResumed}
     * for every row returned here (once auto-resume has picked them up), otherwise
     * these same original crashed rows keep matching this query forever on every
     * future restart — re-triggering auto-resume in an infinite loop even after
     * the table was already fully re-scanned by a later, successful job.
     */
    public synchronized List<ResumableTable> findResumableStaleTables() {
        String sql = """
                SELECT id, job_id, job_ts, table_name, config_json, last_pk_checkpoint,
                       source_count, dest_count, diff_count
                FROM job_tables
                WHERE status = 'FAILED'
                  AND error_msg = 'Interrupted by application restart (job was RUNNING when the app stopped)'
                  AND config_json IS NOT NULL
                ORDER BY job_id ASC, id ASC
                """;
        List<ResumableTable> out = new ArrayList<>();
        try (Connection c = conn(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                out.add(new ResumableTable(
                        rs.getLong("id"),
                        rs.getLong("job_id"),
                        rs.getString("job_ts"),
                        rs.getString("table_name"),
                        rs.getString("config_json"),
                        rs.getString("last_pk_checkpoint"),
                        rs.getLong("source_count"),
                        rs.getLong("dest_count"),
                        rs.getLong("diff_count")
                ));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find resumable stale tables", e);
        }
        return out;
    }

    /**
     * Marks original crashed job_table rows as already picked up for auto-resume,
     * by rewriting their {@code error_msg} so they no longer match the exact-string
     * lookup in {@link #findResumableStaleTables()}. Without this, the same stale
     * rows would be re-discovered and re-resumed on every subsequent app restart,
     * forever — even long after the table was already successfully re-scanned by
     * whatever job actually resumed it.
     */
    public synchronized void markStaleRowsResumed(List<Long> jobTableIds) {
        if (jobTableIds == null || jobTableIds.isEmpty()) return;
        String sql = "UPDATE job_tables SET error_msg = 'Interrupted by application restart (already auto-resumed)' WHERE id = ?";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (Long id : jobTableIds) {
                ps.setLong(1, id);
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark stale rows as resumed", e);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private Map<String, Object> jobFromRow(ResultSet rs) throws SQLException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", rs.getLong("id"));
        m.put("jobTs", rs.getString("job_ts"));
        m.put("status", rs.getString("status"));
        m.put("tableCount", rs.getInt("table_count"));
        m.put("sourceCount", rs.getLong("source_count"));
        m.put("destCount", rs.getLong("dest_count"));
        m.put("diffCount", rs.getLong("diff_count"));
        m.put("error", rs.getString("error_msg"));
        m.put("startedAt", rs.getString("started_at"));
        m.put("finishedAt", rs.getString("finished_at"));
        return m;
    }

    private Map<String, Object> jobTableFromRow(ResultSet rs) throws SQLException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", rs.getLong("id"));
        m.put("jobId", rs.getLong("job_id"));
        m.put("jobTs", rs.getString("job_ts"));
        m.put("tableName", rs.getString("table_name"));
        m.put("status", rs.getString("status"));
        m.put("sourceCount", rs.getLong("source_count"));
        m.put("destCount", rs.getLong("dest_count"));
        m.put("diffCount", rs.getLong("diff_count"));
        m.put("error", rs.getString("error_msg"));
        m.put("startedAt", rs.getString("started_at"));
        m.put("finishedAt", rs.getString("finished_at"));
        m.put("lastPkCheckpoint", rs.getString("last_pk_checkpoint"));
        m.put("totalEstimate", rs.getLong("total_estimate"));
        return m;
    }

    private Connection conn() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    /** Latest job_ts by insertion order (includes RUNNING/FAILED/COMPLETED). */
    public synchronized String latestJobTs() {
        String sql = "SELECT job_ts FROM jobs ORDER BY id DESC LIMIT 1";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getString(1) : null;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to fetch latest job_ts", e);
        }
    }

    /** Latest completed job_ts for stable historical view fallback. */
    public synchronized String latestCompletedJobTs() {
        String sql = "SELECT job_ts FROM jobs WHERE status = 'COMPLETED' ORDER BY id DESC LIMIT 1";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getString(1) : null;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to fetch latest completed job_ts", e);
        }
    }
}
