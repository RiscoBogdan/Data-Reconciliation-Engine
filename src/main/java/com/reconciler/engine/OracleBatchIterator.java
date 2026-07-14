package com.reconciler.engine;

import com.reconciler.config.JobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * Keyset-paginated ("seek method") iterator over an Oracle table.
 *
 * <h3>Why keyset pagination instead of a long-running cursor?</h3>
 * <ul>
 *   <li>Each page is an independent, short-lived JDBC round-trip — no open
 *       server-side cursor to time out on multi-hour scans.</li>
 *   <li>Oracle's B-tree index on the primary key satisfies
 *       {@code WHERE (pk1, pk2) > (?, ?) FETCH FIRST N ROWS ONLY}
 *       with an index-range scan — the DB reads only the requested page,
 *       not all preceding rows (unlike OFFSET).</li>
 *   <li>Heap usage is bounded to exactly one page at a time.</li>
 * </ul>
 *
 * <h3>Two pre-compiled SQL templates</h3>
 * <pre>{@code
 * ── firstPageSql (no seek condition) ────────────────────────────────────
 * SELECT  <pk_composite> AS pk_composite,
 *         <row_hash>     AS row_hash,
 *         pk_col1, pk_col2            -- raw columns for next-page seek
 * FROM    SCHEMA.TABLE
 * [WHERE  modified_col < CURRENT_TIMESTAMP - INTERVAL '15' MINUTE]
 * ORDER BY pk_col1, pk_col2
 * FETCH FIRST 50000 ROWS ONLY
 *
 * ── seekPageSql (keyset seek with ? bind params) ─────────────────────────
 * SELECT  ...same projection...
 * FROM    SCHEMA.TABLE
 * WHERE   [modified_col < ...]
 *   AND   (pk_col1, pk_col2) > (?, ?)     ← typed bind params, no string inlining
 * ORDER BY pk_col1, pk_col2
 * FETCH FIRST 50000 ROWS ONLY
 * }</pre>
 *
 * <h3>Why two templates instead of one?</h3>
 * Oracle's cursor-sharing cache (shared pool) reuses an execution plan for
 * PreparedStatements that share the same SQL text.  Pre-compiling both
 * templates means only two parses happen for the entire iterator lifetime,
 * regardless of how many millions of rows are scanned.
 *
 * <h3>Type-safe bind parameters</h3>
 * PK column values are captured via {@code rs.getObject(col)} (preserving the
 * JDBC-mapped Java type — {@code Long} for NUMBER, {@code String} for VARCHAR2,
 * {@code Timestamp} for DATE/TIMESTAMP) and re-bound via
 * {@code ps.setObject(i, val)}.  No string-quoting, no implicit cast, no
 * SQL injection surface.
 *
 * <p>The iterator reuses a single {@link Connection}; call {@link #close()} to
 * release it when done (try-with-resources recommended).
 */
public final class OracleBatchIterator implements Iterator<Row>, Closeable {

    private static final Logger log = LoggerFactory.getLogger(OracleBatchIterator.class);

    /** Rows fetched per round-trip to Oracle. Tune via env var ORACLE_BATCH_SIZE. */
    private static final int BATCH_SIZE = Integer.parseInt(
            System.getenv().getOrDefault("ORACLE_BATCH_SIZE", "200"));

    private final Connection       conn;
    private final List<String>     keyColumns;
    private final Map<String, String> typeByColumn;

    // ── Pre-compiled SQL templates ───────────────────────────────────────

    /** Used for the very first batch — no keyset condition. */
    private final PreparedStatement firstPagePs;

    /**
     * Used for all subsequent batches — keyset condition with {@code ?} bind
     * params in the same positions as {@link #keyColumns}.
     */
    private final PreparedStatement seekPagePs;

    // ── Sliding buffer ───────────────────────────────────────────────────

    private final List<Row> buffer = new ArrayList<>(BATCH_SIZE);
    private int bufferIdx = 0;

    /**
     * Native Java objects for the last row's PK columns (e.g. {@code Long},
     * {@code String}, {@code Timestamp}).  {@code null} = first page not yet fetched.
     */
    private List<Object> lastPkObjects = null;

    /** {@code true} when the DB returns fewer rows than {@code BATCH_SIZE}. */
    private boolean exhausted = false;

    // ── Constructor ──────────────────────────────────────────────────────

    /**
     * Builds both SQL templates and pre-compiles them into {@link PreparedStatement}s.
     * Oracle parses each statement exactly once; subsequent pages reuse the cached plan.
     *
     * @param conn   an open Oracle JDBC connection (owned by this iterator)
     * @param config job configuration
     */
    public OracleBatchIterator(Connection conn, JobConfig config) throws SQLException {
        this(conn, config, null);
    }

    /**
     * Same as {@link #OracleBatchIterator(Connection, JobConfig)} but resumable:
     * {@code resumeAfterPk}, if non-null, seeds the first page with
     * {@code WHERE <pk_composite_expr> > ?} bound to that value, so a restarted
     * scan picks up past the last checkpointed key instead of re-scanning the
     * whole table. Subsequent pages then continue via the normal typed keyset
     * seek exactly as before.
     */
    public OracleBatchIterator(Connection conn, JobConfig config, String resumeAfterPk) throws SQLException {
        this.conn       = conn;
        this.keyColumns = config.keyColumns();
        this.typeByColumn = loadTypeMap(conn, config.sourceTable());

        // Validate every key/hash identifier before it's concatenated into SQL (S2).
        this.keyColumns.forEach(QueryStrategy::requireSafeIdentifier);
        List<String> hashCols = (config.hashColumns() != null && !config.hashColumns().isEmpty())
                ? config.hashColumns()
                : keyColumns;
        hashCols.forEach(QueryStrategy::requireSafeIdentifier);

        String pkExpr   = buildPkExpr();
        String hashExpr = buildHashExpr(hashCols);
        String orderBy  = String.join(", ", keyColumns);
        // Validate the table identifier before it's concatenated into SQL (S2).
        String table    = QueryStrategy.requireSafeIdentifier(config.sourceTable());

        // ── Shared SELECT / ORDER BY fragments ───────────────────────────
        StringBuilder selectClause = new StringBuilder("SELECT\n");
        selectClause.append("    ").append(pkExpr).append(" AS pk_composite,\n");
        selectClause.append("    RAWTOHEX(STANDARD_HASH(").append(hashExpr).append(", 'MD5')) AS row_hash");
        for (String col : keyColumns) {
            selectClause.append(",\n    ").append(col); // raw columns for keyset tracking
        }
        selectClause.append("\nFROM ").append(table).append("\n");

        String bufferFilter = "";
        if (config.modifiedColumn() != null && !config.modifiedColumn().isBlank()) {
            bufferFilter = QueryStrategy.requireSafeIdentifier(config.modifiedColumn())
                    + " < CURRENT_TIMESTAMP - INTERVAL '"
                    + config.effectiveBufferMinutes() + "' MINUTE";
        }

        String orderAndFetch = "ORDER BY " + orderBy + "\nFETCH FIRST " + BATCH_SIZE + " ROWS ONLY";

        // ── Template 1: first page — no typed keyset condition, but may include
        //    a resume filter using the SAME typed row-constructor as the seek
        //    pages below (not a plain string comparison on pk_composite —
        //    that would sort numeric keys lexicographically, e.g. treating
        //    "250" as greater than "1500"), so a restarted scan resumes at
        //    exactly the right numeric/typed position.
        boolean hasResume = resumeAfterPk != null && !resumeAfterPk.isBlank();
        String keysetCondition = buildKeysetTemplate(); // (pk1, pk2) > (?, ?) or pk1 > ?

        StringBuilder firstSql = new StringBuilder(selectClause);
        List<String> firstPredicates = new ArrayList<>();
        if (!bufferFilter.isBlank()) firstPredicates.add(bufferFilter);
        if (hasResume) firstPredicates.add(keysetCondition);
        if (!firstPredicates.isEmpty()) {
            firstSql.append("WHERE ").append(String.join("\n  AND ", firstPredicates)).append("\n");
        }
        firstSql.append(orderAndFetch);

        // ── Template 2: seek pages — keyset condition with ? params ───────
        StringBuilder seekSql = new StringBuilder(selectClause);
        if (!bufferFilter.isBlank()) {
            seekSql.append("WHERE ").append(bufferFilter).append("\n  AND ").append(keysetCondition).append("\n");
        } else {
            seekSql.append("WHERE ").append(keysetCondition).append("\n");
        }
        seekSql.append(orderAndFetch);

        log.debug("Oracle firstPageSql:\n{}", firstSql);
        log.debug("Oracle seekPageSql:\n{}", seekSql);

        this.firstPagePs = conn.prepareStatement(firstSql.toString());
        this.seekPagePs  = conn.prepareStatement(seekSql.toString());

        if (hasResume) {
            // Bind each part of the composite resume key using its NATIVE typed
            // value (e.g. BigDecimal for NUMBER columns), not the raw string —
            // otherwise Oracle compares "250" > "1500" as text and gets it backwards.
            List<Object> resumeValues = parseResumeValues(resumeAfterPk);
            for (int i = 0; i < resumeValues.size(); i++) {
                firstPagePs.setObject(i + 1, resumeValues.get(i));
            }
        }

        firstPagePs.setFetchSize(BATCH_SIZE);
        seekPagePs.setFetchSize(BATCH_SIZE);
    }

    // ── Iterator contract ────────────────────────────────────────────────

    @Override
    public boolean hasNext() {
        if (bufferIdx < buffer.size()) return true;
        if (exhausted) return false;
        fetchNextBatch();
        return bufferIdx < buffer.size();
    }

    @Override
    public Row next() {
        if (!hasNext()) throw new NoSuchElementException("OracleBatchIterator exhausted");
        return buffer.get(bufferIdx++);
    }

    @Override
    public void close() {
        try { firstPagePs.close(); } catch (SQLException ignored) {}
        try { seekPagePs.close();  } catch (SQLException ignored) {}
        try { conn.close();        } catch (SQLException ignored) {}
        log.debug("OracleBatchIterator closed");
    }

    // ── Private helpers ──────────────────────────────────────────────────

    /**
     * Issues one paged round-trip and fills {@link #buffer}.
     * Selects the pre-compiled statement based on whether a seek position exists.
     */
    private void fetchNextBatch() {
        buffer.clear();
        bufferIdx = 0;

        try {
            PreparedStatement ps;

            if (lastPkObjects == null) {
                // ── First page: no bind params needed ────────────────────
                ps = firstPagePs;
            } else {
                // ── Seek page: bind last PK values as typed objects ───────
                ps = seekPagePs;
                for (int i = 0; i < lastPkObjects.size(); i++) {
                    ps.setObject(i + 1, lastPkObjects.get(i));
                }
            }

            try (ResultSet rs = ps.executeQuery()) {
                int count = 0;
                while (rs.next()) {
                    buffer.add(new Row(
                            rs.getString("pk_composite"),
                            rs.getString("row_hash")));

                    // Capture PK values as native Java objects — preserves the
                    // actual JDBC type (Long, String, Timestamp, BigDecimal…)
                    // so the next page's setObject() call is type-safe.
                    List<Object> pkObjs = new ArrayList<>(keyColumns.size());
                    for (String col : keyColumns) {
                        pkObjs.add(rs.getObject(col));
                    }
                    lastPkObjects = pkObjs;
                    count++;
                }

                if (count < BATCH_SIZE) {
                    exhausted = true;
                    log.debug("Oracle iterator exhausted; final batch={} rows", count);
                } else {
                    log.debug("Oracle batch fetched {} rows; last PK={}", count, lastPkObjects);
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("Oracle batch fetch failed", e);
        }
    }

    // ── SQL fragment builders ────────────────────────────────────────────

    /**
     * {@code CAST(pk1 AS VARCHAR2(4000)) || <PK_DELIMITER> || CAST(pk2 ...)}
     * Uses {@link QueryStrategy#PK_DELIMITER} (a control character, not the
     * printable {@code '~'}) so that a string PK column whose actual value
     * contains {@code '~'} can't corrupt the composite key / resume split.
     */
    private String buildPkExpr() {
        String sep = "'" + QueryStrategy.PK_DELIMITER + "'";
        return keyColumns.stream()
                .map(k -> "CAST(" + k + " AS VARCHAR2(4000))")
                .collect(Collectors.joining(" || " + sep + " || "));
    }

    /** Stable, type-aware normalization for Oracle hash input. */
    private String buildHashExpr(List<String> cols) {
        return cols.stream()
                .map(this::normalizeForHash)
                .collect(Collectors.joining(" || '" + QueryStrategy.HASH_DELIMITER + "' || "));
    }

    private String normalizeForHash(String column) {
        String type = typeByColumn.getOrDefault(column.toUpperCase(Locale.ROOT), "VARCHAR2");
        // Delegates to the shared HashNormalizer so this scan path stays
        // byte-for-byte identical to SnowflakeQueryStrategy (CODE_REVIEW C4).
        return HashNormalizer.oracle(column, type);
    }

    private Map<String, String> loadTypeMap(Connection connection, String tableRef) throws SQLException {
        String[] parts = tableRef.toUpperCase(Locale.ROOT).split("\\.");
        String schema;
        String table;
        if (parts.length >= 2) {
            schema = parts[parts.length - 2];
            table = parts[parts.length - 1];
        } else {
            schema = null;
            table = parts[0];
        }

        Map<String, String> out = new HashMap<>();
        DatabaseMetaData md = connection.getMetaData();
        try (ResultSet rs = md.getColumns(null, schema, table, null)) {
            while (rs.next()) {
                out.put(rs.getString("COLUMN_NAME").toUpperCase(Locale.ROOT),
                        rs.getString("TYPE_NAME").toUpperCase(Locale.ROOT));
            }
        }
        return out;
    }

    /**
     * Splits the composite {@code resumeAfterPk} string back into one
     * native-typed value per key column, so it can be bound with
     * {@code setObject} exactly like a normal seek-page keyset param.
     * NUMBER-typed columns parse to {@link java.math.BigDecimal} so Oracle
     * compares them numerically instead of as lexicographic strings.
     *
     * <p>Uses {@link QueryStrategy#splitComposite} — the SAME delimiter-aware
     * split used everywhere else the composite key is built/parsed, so a
     * string PK column whose value happens to contain {@code '~'} can't
     * misalign the other columns (see {@link QueryStrategy#PK_DELIMITER}).
     */
    private List<Object> parseResumeValues(String resumeAfterPk) {
        String[] parts = QueryStrategy.splitComposite(resumeAfterPk, keyColumns.size());
        List<Object> out = new ArrayList<>(keyColumns.size());
        for (int i = 0; i < keyColumns.size(); i++) {
            // If splitting failed (parts == null — e.g. a corrupt/unexpected
            // legacy checkpoint), fall back to treating the WHOLE string as
            // the first column's value and leave the rest blank. This can't
            // produce a wrong-numeric-comparison bug (the worst case is an
            // overly-conservative resume point), it just may not resume at
            // the exact right row for extremely old checkpoints.
            String val = parts != null ? parts[i] : (i == 0 ? resumeAfterPk : "");
            String type = typeByColumn.getOrDefault(keyColumns.get(i).toUpperCase(Locale.ROOT), "VARCHAR2");
            boolean numeric = type.contains("NUMBER") || type.contains("DECIMAL")
                    || type.contains("NUMERIC") || type.contains("FLOAT");
            if (numeric && !val.isBlank()) {
                try {
                    out.add(new java.math.BigDecimal(val));
                    continue;
                } catch (NumberFormatException ignored) {
                    // Fall through to string bind below.
                }
            }
            out.add(val);
        }
        return out;
    }

    /**
     * Builds the keyset WHERE template with {@code ?} placeholders.
     *
     * <ul>
     *   <li>Single key  → {@code pk1 > ?}</li>
     *   <li>Composite   → {@code (pk1, pk2, pk3) > (?, ?, ?)}</li>
     * </ul>
     *
     * Oracle 12c+ supports row-value constructors in WHERE with bind parameters,
     * and the optimiser resolves to an index range scan on the PK index.
     */
    private String buildKeysetTemplate() {
        if (keyColumns.size() == 1) {
            return keyColumns.getFirst() + " > ?";
        }
        String cols = String.join(", ", keyColumns);
        String params = keyColumns.stream().map(col -> "?").collect(Collectors.joining(", "));
        return "(" + cols + ") > (" + params + ")";
    }
}
