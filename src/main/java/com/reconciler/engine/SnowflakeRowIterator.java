package com.reconciler.engine;

import com.reconciler.config.AppProperties;
import com.reconciler.config.JobConfig;
import net.snowflake.client.jdbc.SnowflakeResultSet;
import net.snowflake.client.jdbc.SnowflakeResultSetSerializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.*;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;

/**
 * Snowflake chunk-based iterator using the native
 * {@link SnowflakeResultSetSerializable} API.
 *
 * <h3>How it works</h3>
 * <ol>
 *   <li>The query (with {@code ORDER BY}) is submitted to the warehouse and
 *       executed entirely server-side.  The sorted result is materialised into
 *       encrypted, compressed <b>Arrow chunks</b> stored in Snowflake's
 *       internal cloud storage.</li>
 *   <li>{@code sfResultSet.getResultSetSerializables(chunkBytes)} splits the
 *       full result into a list of <b>chunk metadata pointers</b> (no data
 *       transferred yet).  Each pointer is an independent, self-contained
 *       mini-{@code ResultSet} descriptor.</li>
 *   <li>This iterator walks through the chunk list sequentially, lazily
 *       opening each chunk's {@code ResultSet} only when the previous one is
 *       exhausted.  At most one decompressed chunk (~{@code CHUNK_BYTES}) lives
 *       in the JVM heap at any time.</li>
 * </ol>
 *
 * <h3>Sort-order guarantee</h3>
 * Because Snowflake applies the full {@code ORDER BY} <em>before</em> splitting
 * into chunks, iterating the chunks in list order preserves the global sort —
 * the sort-merge zipper in {@link CompareEngine} stays correct.
 *
 * <h3>Why not Option 2 (AsyncJob) or Option 3 (Snowpark)?</h3>
 * <ul>
 *   <li><b>AsyncJob</b> is part of the Snowpark Java SDK which is designed for
 *       stored procedures running <em>inside</em> the warehouse, not external
 *       client apps.</li>
 *   <li><b>Snowpark / DataFrame.toLocalIterator()</b> requires deploying JARs
 *       to Snowflake stages — heavyweight and incompatible with a standalone
 *       Fat JAR distribution.</li>
 *   <li><b>SnowflakeResultSetSerializable</b> is part of the standard
 *       {@code snowflake-jdbc} driver already on the classpath — zero extra
 *       dependencies.</li>
 * </ul>
 */
public final class SnowflakeRowIterator implements Iterator<Row>, Closeable {

    private static final Logger log = LoggerFactory.getLogger(SnowflakeRowIterator.class);

    /**
     * Target size for each serialisable chunk in bytes.
     * Tune via env var {@code SNOWFLAKE_CHUNK_BYTES} (default 50 MB).
     */
    private static final long CHUNK_BYTES = Long.parseLong(
            System.getenv().getOrDefault("SNOWFLAKE_CHUNK_BYTES",
                    String.valueOf(50L * 1024 * 1024))); // 50 MB

    // ── State ────────────────────────────────────────────────────────────

    private final Connection conn;

    /** Ordered list of chunk metadata pointers returned by Snowflake. */
    private final List<SnowflakeResultSetSerializable> chunks;

    /** Connection properties forwarded to each chunk's ResultSet (user, password, etc.). */
    private final Properties connProps;

    /** Index into {@link #chunks} for the chunk currently being consumed. */
    private int chunkIdx = 0;

    /** JDBC ResultSet for the current chunk (null = not yet opened). */
    private ResultSet currentChunkRs = null;

    /** Pre-fetched next row (look-ahead so hasNext() is idempotent). */
    private Row nextRow = null;

    /** {@code true} when all chunks have been fully consumed. */
    private boolean exhausted = false;

    // ── Factory ──────────────────────────────────────────────────────────

    /**
     * Executes the Snowflake query, splits the result into serialisable chunks,
     * and returns an iterator ready for the first row.
     *
     * @param conn   an open Snowflake JDBC connection (owned by this iterator)
     * @param config job configuration — table name, keys, hash columns, buffer window
     */
    public static SnowflakeRowIterator open(Connection conn,
                                            JobConfig config,
                                            AppProperties props,
                                            List<QueryStrategy.ColumnInfo> columns) throws SQLException {
        return open(conn, config, props, columns, null);
    }

    /**
     * Same as {@link #open(Connection, JobConfig, AppProperties, List)} but
     * resumable: {@code resumeAfterPk}, if non-null, restricts the query to
     * rows past that checkpoint (see {@link QueryStrategy#buildHashQuery(JobConfig, List, String)}).
     */
    public static SnowflakeRowIterator open(Connection conn,
                                            JobConfig config,
                                            AppProperties props,
                                            List<QueryStrategy.ColumnInfo> columns,
                                            String resumeAfterPk) throws SQLException {
        SnowflakeQueryStrategy strategy = new SnowflakeQueryStrategy();
        String sql = strategy.buildHashQuery(config, columns, resumeAfterPk);
        log.info("Snowflake query:\n{}", sql);
        return new SnowflakeRowIterator(conn, sql, props.snowflake().user(), props.snowflake().password());
    }

    // ── Constructor ──────────────────────────────────────────────────────

    private SnowflakeRowIterator(Connection conn, String sql,
                                  String sfUser, String sfPassword) throws SQLException {
        this.conn = conn;
        this.connProps = new Properties();
        connProps.setProperty("user",     sfUser);
        connProps.setProperty("password", sfPassword);

        // Execute the full sorted query — returns immediately after the warehouse
        // finishes computing and stages the result chunks in cloud storage.
        //
        // IMPORTANT (R1): getResultSetSerializables() MUST be called on sfRs
        // *before* the try-with-resources closes the Statement (which closes rs).
        // The serializable chunk descriptors are self-contained once extracted,
        // so they remain valid after the statement/rs close — but the extraction
        // itself has to happen inside this block. Do not move it out.
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);

            // Unwrap to the Snowflake-native ResultSet interface
            SnowflakeResultSet sfRs = rs.unwrap(SnowflakeResultSet.class);

            // Split result into ~CHUNK_BYTES metadata pointers.
            // No actual row data is transferred here — only chunk descriptors.
            this.chunks = sfRs.getResultSetSerializables(CHUNK_BYTES);
        }

        log.info("Snowflake result split into {} chunk(s) of ~{} MB each",
                chunks.size(), CHUNK_BYTES / 1024 / 1024);
    }

    // ── Iterator contract ────────────────────────────────────────────────

    @Override
    public boolean hasNext() {
        if (exhausted) return false;
        if (nextRow != null) return true;
        advance();
        return !exhausted;
    }

    @Override
    public Row next() {
        if (!hasNext()) throw new NoSuchElementException("SnowflakeRowIterator exhausted");
        Row row = nextRow;
        nextRow = null;
        return row;
    }

    /** Releases the current chunk ResultSet and the JDBC Connection. */
    @Override
    public void close() {
        closeCurrentChunk();
        try { conn.close(); } catch (SQLException ignored) {}
        log.debug("SnowflakeRowIterator closed ({} chunk(s) total)", chunks.size());
    }

    // ── Private helpers ──────────────────────────────────────────────────

    /**
     * Advances to the next available row across chunk boundaries.
     * Sets {@link #nextRow} if a row is found, or {@link #exhausted} if done.
     *
     * <p>{@code @SuppressWarnings("deprecation")} — both {@code getResultSet()}
     * overloads on {@link SnowflakeResultSetSerializable} are marked
     * {@code @Deprecated} internally by Snowflake as they plan to unify the
     * API in a future driver version, but no non-deprecated replacement exists
     * yet (driver 3.x).  This is still the correct, production-supported call.
     */
    @SuppressWarnings("deprecation")
    private void advance() {
        while (true) {
            // ── Try the current chunk first ──────────────────────────────
            if (currentChunkRs != null) {
                try {
                    if (currentChunkRs.next()) {
                        // Read by index — chunk ResultSets from getResultSet(Properties)
                        // do not reliably preserve column alias names like "pk_composite".
                        nextRow = new Row(
                                currentChunkRs.getString(1),   // pk_composite
                                currentChunkRs.getString(2));  // row_hash
                        return;
                    }
                } catch (SQLException e) {
                    throw new RuntimeException("Snowflake chunk read failed", e);
                }
                // Chunk exhausted — close it and move to the next
                closeCurrentChunk();
                chunkIdx++;
            }

            // ── Open the next chunk ──────────────────────────────────────
            if (chunkIdx >= chunks.size()) {
                exhausted = true;
                log.debug("All {} Snowflake chunks consumed", chunks.size());
                return;
            }

            try {
                log.debug("Opening Snowflake chunk {}/{}", chunkIdx + 1, chunks.size());
                // getResultSet(Properties) is the non-deprecated API — downloads
                // and decompresses this chunk from Snowflake's cloud storage.
                currentChunkRs = chunks.get(chunkIdx).getResultSet(connProps);
            } catch (SQLException e) {
                throw new RuntimeException(
                        "Failed to open Snowflake chunk " + chunkIdx, e);
            }
            // Loop back to read from the freshly opened chunk
        }
    }

    private void closeCurrentChunk() {
        if (currentChunkRs != null) {
            try { currentChunkRs.close(); } catch (SQLException ignored) {}
            currentChunkRs = null;
        }
    }

    // ── Chunk exposure for parallel use ─────────────────────────────────

    /**
     * Returns the raw chunk list for callers that want to distribute chunks
     * across threads.  Each chunk is a self-contained, independent unit —
     * safe to pass to a separate {@link Thread} or virtual thread.
     *
     * <p><b>Note:</b> if you consume chunks directly via this method, do
     * <em>not</em> also call {@link #hasNext()} / {@link #next()} — they
     * share the same chunk list.
     *
     * @return immutable view of the serialisable chunk pointers
     */
    public List<SnowflakeResultSetSerializable> getChunks() {
        return List.copyOf(chunks);
    }
}
