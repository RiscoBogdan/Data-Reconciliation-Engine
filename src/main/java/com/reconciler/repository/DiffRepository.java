package com.reconciler.repository;

import com.reconciler.config.AppProperties;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.util.*;
import java.util.function.Consumer;

/**
 * Spring-managed repository that logs row-level differences into an embedded SQLite database.
 * Thread-safe: all public methods are {@code synchronized} (SQLite allows one writer at a time).
 */
@Repository
public class DiffRepository {

    private final String jdbcUrl;

    public DiffRepository(AppProperties props) {
        this.jdbcUrl = "jdbc:sqlite:" + props.sqlite().path();
        initialise();
    }

    /** Initialise / migrate schema (idempotent). */
    public synchronized void initialise() {
        try (Connection c = conn(); Statement s = c.createStatement()) {
            // WAL + a generous busy timeout so the UI's concurrent reads don't
            // collide with the scan's writes and throw SQLITE_BUSY (R4). WAL
            // allows one writer + many readers simultaneously.
            s.execute("PRAGMA journal_mode=WAL");
            s.execute("PRAGMA busy_timeout=30000");
            s.execute("PRAGMA synchronous=NORMAL");
            s.execute("""
                CREATE TABLE IF NOT EXISTS diffs (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_ts         TEXT    NOT NULL,
                    table_name     TEXT    NOT NULL DEFAULT '',
                    pk_composite   TEXT    NOT NULL,
                    diff_type      TEXT    NOT NULL,
                    source_hash    TEXT,
                    dest_hash      TEXT,
                    changed_columns TEXT,
                    created_at     TEXT    NOT NULL DEFAULT (datetime('now'))
                )
            """);
            // lightweight migration for existing DBs
            try {
                s.execute("ALTER TABLE diffs ADD COLUMN changed_columns TEXT");
            } catch (SQLException ignored) {
                // column already exists
            }
            try {
                s.execute("ALTER TABLE diffs ADD COLUMN table_name TEXT NOT NULL DEFAULT ''");
            } catch (SQLException ignored) {
                // column already exists
            }

            s.execute("CREATE INDEX IF NOT EXISTS idx_diffs_pk ON diffs(pk_composite)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_diffs_type ON diffs(diff_type)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_diffs_table ON diffs(table_name)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_diffs_job_ts ON diffs(job_ts)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_diffs_job_table_id ON diffs(job_ts, table_name, id DESC)");
            // Speeds up per-column filtering + keeps deep-page (LIMIT/OFFSET) scans fast.
            s.execute("CREATE INDEX IF NOT EXISTS idx_diffs_job_type ON diffs(job_ts, diff_type)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_diffs_job_pk ON diffs(job_ts, pk_composite)");
            // Prevent duplicate diffs within the same job for the same PK+table
            s.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_diffs_unique_pk ON diffs(job_ts, table_name, pk_composite)");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialise SQLite", e);
        }
    }

    /** Insert a single diff record. */
    public synchronized void insert(String jobTs, String pk, String diffType,
                                    String sourceHash, String destHash, String changedColumns) {
        insert(jobTs, "", pk, diffType, sourceHash, destHash, changedColumns);
    }

    public synchronized void insert(String jobTs,
                                    String tableName,
                                    String pk,
                                    String diffType,
                                    String sourceHash,
                                    String destHash,
                                    String changedColumns) {
        String sql = "INSERT OR REPLACE INTO diffs(job_ts, table_name, pk_composite, diff_type, source_hash, dest_hash, changed_columns) VALUES(?,?,?,?,?,?,?)";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, jobTs);
            ps.setString(2, tableName == null ? "" : tableName);
            ps.setString(3, pk);
            ps.setString(4, diffType);
            ps.setString(5, sourceHash);
            ps.setString(6, destHash);
            ps.setString(7, changedColumns);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to insert diff", e);
        }
    }

    /** Batch insert for performance (called from the engine). */
    public synchronized void insertBatch(String jobTs, List<DiffRecord> records) {
        String sql = "INSERT OR REPLACE INTO diffs(job_ts, table_name, pk_composite, diff_type, source_hash, dest_hash, changed_columns) VALUES(?,?,?,?,?,?,?)";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            c.setAutoCommit(false);
            for (DiffRecord r : records) {
                ps.setString(1, jobTs);
                ps.setString(2, r.tableName());
                ps.setString(3, r.pk());
                ps.setString(4, r.type());
                ps.setString(5, r.sourceHash());
                ps.setString(6, r.destHash());
                ps.setString(7, r.changedColumns());
                ps.addBatch();
            }
            ps.executeBatch();
            c.commit();
        } catch (SQLException e) {
            throw new RuntimeException("Batch insert failed", e);
        }
    }

    /** Fetch a paginated slice of diffs, optionally filtered by search term. */
    public synchronized List<Map<String, Object>> fetch(int page, int size, String search, String tableName) {
        return fetch(page, size, search, tableName, null);
    }

    /** Fetch a paginated slice of diffs (all history), with optional per-column filters. */
    public synchronized List<Map<String, Object>> fetch(int page, int size, String search, String tableName, Filters filters) {
        int offset = (page - 1) * size;
        StringBuilder where = new StringBuilder();
        List<String> params = new ArrayList<>();
        appendGlobalFilters(where, params, search, tableName, filters);

        String dedupClause = (where.isEmpty() ? " WHERE " : " AND ") +
                "id = (SELECT MAX(d2.id) FROM diffs d2 WHERE d2.table_name = diffs.table_name AND d2.pk_composite = diffs.pk_composite AND COALESCE(d2.source_hash,'') = COALESCE(diffs.source_hash,'') AND COALESCE(d2.dest_hash,'') = COALESCE(diffs.dest_hash,''))";
        String sql = SELECT_COLS + where + dedupClause + " ORDER BY id DESC LIMIT ? OFFSET ?";

        List<Map<String, Object>> rows = new ArrayList<>();
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            int idx = bind(ps, params);
            ps.setInt(idx++, size);
            ps.setInt(idx, offset);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(mapRow(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Fetch failed", e);
        }
        return rows;
    }

    /** Total count of diffs matching optional search. */
    public synchronized int count(String search, String tableName) {
        return count(search, tableName, null);
    }

    /** Total count of diffs (all history) matching optional search + per-column filters. */
    public synchronized int count(String search, String tableName, Filters filters) {
        StringBuilder where = new StringBuilder();
        List<String> params = new ArrayList<>();
        appendGlobalFilters(where, params, search, tableName, filters);

        String dedupClause = (where.isEmpty() ? " WHERE " : " AND ") +
                "id = (SELECT MAX(d2.id) FROM diffs d2 WHERE d2.table_name = diffs.table_name AND d2.pk_composite = diffs.pk_composite AND COALESCE(d2.source_hash,'') = COALESCE(diffs.source_hash,'') AND COALESCE(d2.dest_hash,'') = COALESCE(diffs.dest_hash,''))";
        String sql = "SELECT COUNT(*) FROM diffs" + where + dedupClause;
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Count failed", e);
        }
    }

    /** Wipe all diff records. */
    public synchronized void clear() {
        try (Connection c = conn(); Statement s = c.createStatement()) {
            s.execute("DELETE FROM diffs");
        } catch (SQLException e) {
            throw new RuntimeException("Clear failed", e);
        }
    }

    public synchronized List<String> tableNames() {
        return tableNamesByJobTs(null);
    }

    /** Distinct table names scoped to optional job_ts. */
    public synchronized List<String> tableNamesByJobTs(String jobTs) {
        boolean scoped = jobTs != null && !jobTs.isBlank();
        String sql = scoped
                ? "SELECT DISTINCT table_name FROM diffs WHERE job_ts = ? AND table_name IS NOT NULL AND table_name <> '' ORDER BY table_name"
                : "SELECT DISTINCT table_name FROM diffs WHERE table_name IS NOT NULL AND table_name <> '' ORDER BY table_name";
        List<String> out = new ArrayList<>();
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            if (scoped) ps.setString(1, jobTs);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list table names", e);
        }
        return out;
    }

    public synchronized List<Map<String, Object>> tableSummaries(String search) {
        return tableSummariesByJobTs(search, null);
    }

    /** Aggregate per-table diff stats scoped to optional job_ts. */
    public synchronized List<Map<String, Object>> tableSummariesByJobTs(String search, String jobTs) {
        boolean hasSearch = search != null && !search.isBlank();
        boolean scoped = jobTs != null && !jobTs.isBlank();

        StringBuilder where = new StringBuilder();
        if (scoped) where.append(" WHERE job_ts = ?");
        if (hasSearch) {
            where.append(where.isEmpty() ? " WHERE " : " AND ")
                    .append("(pk_composite LIKE ? OR diff_type LIKE ? OR changed_columns LIKE ? OR table_name LIKE ?)");
        }

        // When not scoped to a single job, deduplicate by table+pk+hashes (keep latest)
        if (!scoped) {
            String dedupWhere = where.isEmpty() ? " WHERE " : " AND ";
            where.append(dedupWhere)
                    .append("id = (SELECT MAX(d2.id) FROM diffs d2 WHERE d2.table_name = diffs.table_name AND d2.pk_composite = diffs.pk_composite AND COALESCE(d2.source_hash,'') = COALESCE(diffs.source_hash,'') AND COALESCE(d2.dest_hash,'') = COALESCE(diffs.dest_hash,''))");
        }

        String sql = """
                SELECT table_name,
                       COUNT(*) AS total,
                       SUM(CASE WHEN diff_type = 'HASH_MISMATCH' THEN 1 ELSE 0 END) AS hash_mismatch,
                       SUM(CASE WHEN diff_type = 'MISSING_IN_DEST' THEN 1 ELSE 0 END) AS missing_in_dest,
                       SUM(CASE WHEN diff_type = 'MISSING_IN_SOURCE' THEN 1 ELSE 0 END) AS missing_in_source
                FROM diffs
                """ + where + " GROUP BY table_name ORDER BY table_name";

        List<Map<String, Object>> out = new ArrayList<>();
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            int idx = 1;
            if (scoped) ps.setString(idx++, jobTs);
            if (hasSearch) {
                String pattern = "%" + search + "%";
                ps.setString(idx++, pattern);
                ps.setString(idx++, pattern);
                ps.setString(idx++, pattern);
                ps.setString(idx, pattern);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(Map.of(
                            "tableName", Objects.toString(rs.getString("table_name"), ""),
                            "total", rs.getInt("total"),
                            "hashMismatch", rs.getInt("hash_mismatch"),
                            "missingInDest", rs.getInt("missing_in_dest"),
                            "missingInSource", rs.getInt("missing_in_source")
                    ));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to summarize diffs by table", e);
        }
        return out;
    }

    private Connection conn() throws SQLException {
        Connection c = DriverManager.getConnection(jdbcUrl);
        // busy_timeout is per-connection, so (re)apply it on every open so no
        // connection ever fails fast with SQLITE_BUSY under the parallel batch
        // path where many writers/readers overlap (R4).
        try (Statement s = c.createStatement()) {
            s.execute("PRAGMA busy_timeout=30000");
        }
        return c;
    }

    /** Fetch diffs scoped to a specific job_ts, with optional table/search filtering. */
    public synchronized List<Map<String, Object>> fetchByJobTs(String jobTs, int page, int size, String search, String tableName) {
        return fetchByJobTs(jobTs, page, size, search, tableName, null);
    }

    /** Count diffs scoped to a specific job_ts. */
    public synchronized int countByJobTs(String jobTs, String search, String tableName) {
        return countByJobTs(jobTs, search, tableName, null);
    }

    /** Delete diffs for a specific job_ts. */
    public synchronized void clearByJobTs(String jobTs) {
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement("DELETE FROM diffs WHERE job_ts = ?")) {
            ps.setString(1, jobTs);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("clearByJobTs failed", e);
        }
    }

    /** Latest job_ts directly from the diffs table (fallback when jobs table has no rows). */
    public synchronized String latestDiffJobTs() {
        String sql = "SELECT job_ts FROM diffs ORDER BY id DESC LIMIT 1";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getString(1) : null;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to fetch latest diff job_ts", e);
        }
    }

    /** Simple carrier for batch inserts. */
    public record DiffRecord(String tableName, String pk, String type, String sourceHash, String destHash, String changedColumns) {}

    /**
     * Per-column search filters for the Difference Log. Any null/blank field is ignored.
     * All terms are matched with case-insensitive {@code LIKE '%term%'}.
     */
    public record Filters(String pk, String diffType, String sourceHash, String destHash,
                          String changedColumns, String table) {
        boolean isEmpty() {
            return blank(pk) && blank(diffType) && blank(sourceHash)
                    && blank(destHash) && blank(changedColumns) && blank(table);
        }
        private static boolean blank(String s) { return s == null || s.isBlank(); }
    }

    /** Append column-specific "AND col LIKE ?" clauses; returns the ordered param list. */
    private static List<String> appendColumnFilters(StringBuilder where, Filters f) {
        List<String> params = new ArrayList<>();
        if (f == null) return params;
        if (!Filters.blank(f.pk()))             { where.append(" AND pk_composite LIKE ?");    params.add("%" + f.pk() + "%"); }
        if (!Filters.blank(f.diffType()))       { where.append(" AND diff_type LIKE ?");       params.add("%" + f.diffType() + "%"); }
        if (!Filters.blank(f.sourceHash()))     { where.append(" AND source_hash LIKE ?");     params.add("%" + f.sourceHash() + "%"); }
        if (!Filters.blank(f.destHash()))       { where.append(" AND dest_hash LIKE ?");       params.add("%" + f.destHash() + "%"); }
        if (!Filters.blank(f.changedColumns())) { where.append(" AND changed_columns LIKE ?"); params.add("%" + f.changedColumns() + "%"); }
        if (!Filters.blank(f.table()))          { where.append(" AND table_name LIKE ?");      params.add("%" + f.table() + "%"); }
        return params;
    }

    private static final String SELECT_COLS =
            "SELECT id, job_ts, table_name, pk_composite, diff_type, source_hash, dest_hash, changed_columns, created_at FROM diffs";

    private static Map<String, Object> mapRow(ResultSet rs) throws SQLException {
        return Map.of(
                "id", rs.getInt("id"),
                "jobTs", rs.getString("job_ts"),
                "tableName", Objects.toString(rs.getString("table_name"), ""),
                "pkComposite", rs.getString("pk_composite"),
                "diffType", rs.getString("diff_type"),
                "sourceHash", Objects.toString(rs.getString("source_hash"), ""),
                "destHash", Objects.toString(rs.getString("dest_hash"), ""),
                "changedColumns", Objects.toString(rs.getString("changed_columns"), ""),
                "createdAt", rs.getString("created_at")
        );
    }

    // ── Filtered fetch/count/stream scoped to a job_ts (used by /api/results & export) ──

    public synchronized List<Map<String, Object>> fetchByJobTs(String jobTs, int page, int size,
                                                               String search, String tableName, Filters filters) {
        int offset = (page - 1) * size;
        StringBuilder where = new StringBuilder(" WHERE job_ts = ?");
        List<String> params = new ArrayList<>();
        params.add(jobTs);
        buildLegacyAndColumn(where, params, search, tableName, filters);

        String sql = SELECT_COLS + where + " ORDER BY id DESC LIMIT ? OFFSET ?";
        List<Map<String, Object>> rows = new ArrayList<>();
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            int idx = bind(ps, params);
            ps.setInt(idx++, size);
            ps.setInt(idx, offset);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(mapRow(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException("fetchByJobTs failed", e);
        }
        return rows;
    }

    public synchronized int countByJobTs(String jobTs, String search, String tableName, Filters filters) {
        StringBuilder where = new StringBuilder(" WHERE job_ts = ?");
        List<String> params = new ArrayList<>();
        params.add(jobTs);
        buildLegacyAndColumn(where, params, search, tableName, filters);

        String sql = "SELECT COUNT(*) FROM diffs" + where;
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (SQLException e) {
            throw new RuntimeException("countByJobTs failed", e);
        }
    }

    /**
     * Stream the full filtered result set (no LIMIT) row-by-row so exports of
     * millions of rows never buffer everything in memory. Scope to a job_ts when
     * provided, otherwise deduplicate to the latest row per table/pk/hashes.
     */
    public synchronized void streamFiltered(String jobTs, String search, String tableName,
                                            Filters filters, Consumer<Map<String, Object>> sink) {
        boolean scoped = jobTs != null && !jobTs.isBlank();
        StringBuilder where = new StringBuilder();
        List<String> params = new ArrayList<>();
        if (scoped) {
            where.append(" WHERE job_ts = ?");
            params.add(jobTs);
            buildLegacyAndColumn(where, params, search, tableName, filters);
        } else {
            appendGlobalFilters(where, params, search, tableName, filters);
            where.append(where.isEmpty() ? " WHERE " : " AND ")
                    .append("id = (SELECT MAX(d2.id) FROM diffs d2 WHERE d2.table_name = diffs.table_name AND d2.pk_composite = diffs.pk_composite AND COALESCE(d2.source_hash,'') = COALESCE(diffs.source_hash,'') AND COALESCE(d2.dest_hash,'') = COALESCE(diffs.dest_hash,''))");
        }
        String sql = SELECT_COLS + where + " ORDER BY id DESC";
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setFetchSize(1000);
            bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) sink.accept(mapRow(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException("streamFiltered failed", e);
        }
    }

    /** WHERE for a job-scoped query: legacy global "search" plus per-column filters. */
    private static void buildLegacyAndColumn(StringBuilder where, List<String> params,
                                             String search, String tableName, Filters filters) {
        if (tableName != null && !tableName.isBlank()) {
            where.append(" AND table_name = ?");
            params.add(tableName.toUpperCase(Locale.ROOT));
        }
        if (search != null && !search.isBlank()) {
            where.append(" AND (pk_composite LIKE ? OR diff_type LIKE ? OR changed_columns LIKE ? OR table_name LIKE ?)");
            String p = "%" + search + "%";
            params.add(p); params.add(p); params.add(p); params.add(p);
        }
        params.addAll(appendColumnFilters(where, filters));
    }

    /** WHERE for an unscoped (all-history) query. */
    private static void appendGlobalFilters(StringBuilder where, List<String> params,
                                            String search, String tableName, Filters filters) {
        if (tableName != null && !tableName.isBlank()) {
            where.append(where.isEmpty() ? " WHERE " : " AND ").append("table_name = ?");
            params.add(tableName.toUpperCase(Locale.ROOT));
        }
        if (search != null && !search.isBlank()) {
            where.append(where.isEmpty() ? " WHERE " : " AND ")
                    .append("(pk_composite LIKE ? OR diff_type LIKE ? OR changed_columns LIKE ? OR table_name LIKE ?)");
            String p = "%" + search + "%";
            params.add(p); params.add(p); params.add(p); params.add(p);
        }
        if (filters != null && !filters.isEmpty()) {
            StringBuilder colWhere = new StringBuilder(where.isEmpty() ? " WHERE 1=1" : "");
            params.addAll(appendColumnFilters(colWhere, filters));
            where.append(colWhere);
        }
    }

    private static int bind(PreparedStatement ps, List<String> params) throws SQLException {
        int idx = 1;
        for (String p : params) ps.setString(idx++, p);
        return idx;
    }
}
