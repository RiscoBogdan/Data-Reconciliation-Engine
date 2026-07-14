package com.reconciler.engine;

import com.reconciler.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Fetches table metadata (columns + primary keys) from Oracle and Snowflake
 * using their respective system / information-schema views.
 *
 * <p>Used by the {@code GET /api/schema} endpoint so the UI can display
 * available columns and auto-populate the PK configuration panel.
 */
public final class SchemaInspector {

    private static final Logger log = LoggerFactory.getLogger(SchemaInspector.class);

    public record ColumnMeta(String name, String type, boolean nullable) {}
    public record TableRef(String qualifiedName) {}
    public record SchemaRef(String name) {}

    public record TableSchema(List<ColumnMeta> columns, List<String> primaryKeys) {}

    /** Lists accessible Oracle schemas (owners). */
    public static List<SchemaRef> listOracleSchemas(String search, AppProperties props) throws SQLException {
        String like = search == null ? "" : search.trim().toUpperCase();
        String sql = """
                SELECT username
                FROM   all_users
                WHERE  (? = '' OR username LIKE '%' || UPPER(?) || '%')
                ORDER  BY username
                """;

        try (Connection conn = DriverManager.getConnection(
                props.oracle().jdbcUrl(), props.oracle().user(), props.oracle().password());
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, like);
            ps.setString(2, like);
            List<SchemaRef> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(new SchemaRef(rs.getString(1)));
            }
            return out;
        }
    }

    /** Lists accessible Snowflake schemas for the given database. */
    public static List<SchemaRef> listSnowflakeSchemas(String database,
                                                        String search,
                                                        AppProperties props) throws SQLException {
        String db = (database == null || database.isBlank()) ? null : database.toUpperCase();
        String like = search == null ? "" : search.trim().toUpperCase();
        String infoSchema = (db == null ? "INFORMATION_SCHEMA" : safeIdentifier(db) + ".INFORMATION_SCHEMA");
        String sql = """
                SELECT schema_name
                FROM   %s.schemata
                WHERE  (? = '' OR schema_name LIKE '%%' || UPPER(?) || '%%')
                ORDER  BY schema_name
                """.formatted(infoSchema);

        try (Connection conn = DriverManager.getConnection(
                props.snowflake().jdbcUrl(), props.snowflake().user(), props.snowflake().password());
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, like);
            ps.setString(2, like);
            List<SchemaRef> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(new SchemaRef(rs.getString(1)));
            }
            return out;
        }
    }

    /** Lists Oracle tables for the provided schema (owner). */
    public static List<TableRef> listOracleTables(String schema, String search, AppProperties props) throws SQLException {
        String owner = (schema == null || schema.isBlank()) ? props.oracle().user() : schema;
        String like = search == null ? "" : search.trim().toUpperCase();

        String sql = """
                SELECT owner || '.' || table_name AS qualified_name
                FROM   all_tables
                WHERE  owner = UPPER(?)
                  AND  (? = '' OR table_name LIKE '%' || UPPER(?) || '%')
                ORDER  BY table_name
                """;

        try (Connection conn = DriverManager.getConnection(
                props.oracle().jdbcUrl(), props.oracle().user(), props.oracle().password());
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, owner);
            ps.setString(2, like);
            ps.setString(3, like);

            List<TableRef> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(new TableRef(rs.getString(1)));
            }
            return out;
        }
    }

    /** Lists Snowflake tables for the provided database/schema. */
    public static List<TableRef> listSnowflakeTables(String database,
                                                     String schema,
                                                     String search,
                                                     AppProperties props) throws SQLException {
        String db = (database == null || database.isBlank()) ? null : database.toUpperCase();
        String sch = (schema == null || schema.isBlank()) ? "PUBLIC" : schema.toUpperCase();
        String like = search == null ? "" : search.trim().toUpperCase();

        String infoSchema = (db == null ? "INFORMATION_SCHEMA" : safeIdentifier(db) + ".INFORMATION_SCHEMA");
        String sql = """
                SELECT table_schema || '.' || table_name
                FROM   %s.tables
                WHERE  table_schema = UPPER(?)
                  AND  table_type = 'BASE TABLE'
                  AND  (? = '' OR table_name LIKE '%%' || UPPER(?) || '%%')
                ORDER  BY table_name
                """.formatted(infoSchema);

        try (Connection conn = DriverManager.getConnection(
                props.snowflake().jdbcUrl(), props.snowflake().user(), props.snowflake().password());
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, sch);
            ps.setString(2, like);
            ps.setString(3, like);

            List<TableRef> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String qualified = db == null ? rs.getString(1) : db + "." + rs.getString(1);
                    out.add(new TableRef(qualified));
                }
            }
            return out;
        }
    }

    // ── Public API ───────────────────────────────────────────────────────

    /**
     * Introspects the Oracle source table.
     *
     * @param qualifiedTable fully-qualified Oracle table, e.g. {@code "SCHEMA.ORDERS"}
     */
    public static TableSchema fromOracle(String qualifiedTable, AppProperties props) throws SQLException {
        String[] parts = parseTwo(qualifiedTable);
        String schema  = parts[0];
        String table   = parts[1];

        try (Connection conn = DriverManager.getConnection(
                props.oracle().jdbcUrl(),
                props.oracle().user(),
                props.oracle().password())) {

            List<ColumnMeta> cols = oracleColumns(conn, schema, table);
            List<String>     pks  = oraclePrimaryKeys(conn, schema, table);
            log.info("Oracle schema for {}: {} cols, {} PKs", qualifiedTable, cols.size(), pks.size());
            return new TableSchema(cols, pks);
        }
    }

    /**
     * Introspects the Snowflake destination table.
     *
     * @param qualifiedTable fully-qualified Snowflake table, e.g.
     *                       {@code "DWH.PUBLIC.ORDERS"} or {@code "PUBLIC.ORDERS"}
     */
    public static TableSchema fromSnowflake(String qualifiedTable, AppProperties props) throws SQLException {
        // Snowflake can be DB.SCHEMA.TABLE or SCHEMA.TABLE
        String database = null;
        String schema;
        String table;
        String[] segments = qualifiedTable.toUpperCase().split("\\.");
        if (segments.length >= 3) {
            database = segments[segments.length - 3];
            schema = segments[segments.length - 2];
            table  = segments[segments.length - 1];
        } else if (segments.length == 2) {
            schema = segments[0];
            table  = segments[1];
        } else {
            schema = "PUBLIC";
            table  = segments[0];
        }

        try (Connection conn = DriverManager.getConnection(
                props.snowflake().jdbcUrl(),
                props.snowflake().user(),
                props.snowflake().password())) {

            List<ColumnMeta> cols = snowflakeColumns(conn, database, schema, table);
            List<String>     pks  = snowflakePrimaryKeys(conn, database, schema, table);
            log.info("Snowflake schema for {}: {} cols, {} PKs", qualifiedTable, cols.size(), pks.size());
            return new TableSchema(cols, pks);
        }
    }

    // ── Oracle helpers ───────────────────────────────────────────────────

    private static List<ColumnMeta> oracleColumns(Connection conn,
                                                   String schema,
                                                   String table) throws SQLException {
        String sql = """
                SELECT column_name, data_type, nullable
                FROM   all_tab_columns
                WHERE  owner      = UPPER(?)
                  AND  table_name = UPPER(?)
                ORDER  BY column_id
                """;
        List<ColumnMeta> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new ColumnMeta(
                            rs.getString(1),
                            rs.getString(2),
                            "Y".equals(rs.getString(3))
                    ));
                }
            }
        }
        return result;
    }

    private static List<String> oraclePrimaryKeys(Connection conn,
                                                   String schema,
                                                   String table) throws SQLException {
        String sql = """
                SELECT cols.column_name
                FROM   all_constraints  cons
                JOIN   all_cons_columns cols
                    ON  cons.constraint_name = cols.constraint_name
                    AND cons.owner           = cols.owner
                WHERE  cons.constraint_type = 'P'
                  AND  cons.owner      = UPPER(?)
                  AND  cons.table_name = UPPER(?)
                ORDER  BY cols.position
                """;
        List<String> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) result.add(rs.getString(1));
            }
        }
        return result;
    }

    // ── Snowflake helpers ────────────────────────────────────────────────

    private static List<ColumnMeta> snowflakeColumns(Connection conn,
                                                      String database,
                                                      String schema,
                                                      String table) throws SQLException {
        String infoSchemaPrefix = (database != null && !database.isBlank())
                ? safeIdentifier(database) + ".INFORMATION_SCHEMA"
                : "INFORMATION_SCHEMA";
        String sql = """
                SELECT column_name, data_type, is_nullable
                FROM   %s.COLUMNS
                WHERE  table_schema = UPPER(?)
                  AND  table_name   = UPPER(?)
                ORDER  BY ordinal_position
                """.formatted(infoSchemaPrefix);
        List<ColumnMeta> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new ColumnMeta(
                            rs.getString(1),
                            rs.getString(2),
                            "YES".equalsIgnoreCase(rs.getString(3))
                    ));
                }
            }
        }
        return result;
    }

    private static List<String> snowflakePrimaryKeys(Connection conn,
                                                      String database,
                                                      String schema,
                                                      String table) throws SQLException {
        // Uses SHOW PRIMARY KEYS to avoid INFORMATION_SCHEMA permission gaps.
        String tableRef = (database != null && !database.isBlank())
                ? safeIdentifier(database) + "." + safeIdentifier(schema) + "." + safeIdentifier(table)
                : safeIdentifier(schema) + "." + safeIdentifier(table);

        String sql = "SHOW PRIMARY KEYS IN TABLE " + tableRef;
        List<PkCol> pkCols = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData md = rs.getMetaData();
            int colNameIdx = findColumnIndex(md, "column_name");
            int seqIdx     = findColumnIndex(md, "key_sequence");
            if (colNameIdx < 0 || seqIdx < 0) {
                throw new SQLException("SHOW PRIMARY KEYS returned unexpected columns");
            }

            while (rs.next()) {
                String col = rs.getString(colNameIdx);
                int seq = rs.getInt(seqIdx);
                pkCols.add(new PkCol(seq, col));
            }
        }

        pkCols.sort(Comparator.comparingInt(PkCol::seq));
        return pkCols.stream().map(PkCol::name).toList();
    }

    private record PkCol(int seq, String name) {}

    // ── Utility ──────────────────────────────────────────────────────────

    /**
     * Parses {@code "SCHEMA.TABLE"} into a two-element array.
     * If no dot is found, uses an empty string as schema (current schema).
     */
    private static String[] parseTwo(String qualified) {
        int dot = qualified.lastIndexOf('.');
        if (dot < 0) return new String[]{"", qualified.toUpperCase()};
        return new String[]{
                qualified.substring(0, dot).toUpperCase(),
                qualified.substring(dot + 1).toUpperCase()
        };
    }

    private static int findColumnIndex(ResultSetMetaData md, String expectedLowerCase) throws SQLException {
        for (int i = 1; i <= md.getColumnCount(); i++) {
            String label = md.getColumnLabel(i);
            if (label != null && label.equalsIgnoreCase(expectedLowerCase)) return i;
            String name = md.getColumnName(i);
            if (name != null && name.equalsIgnoreCase(expectedLowerCase)) return i;
        }
        return -1;
    }

    private static String safeIdentifier(String ident) {
        String upper = ident == null ? "" : ident.trim().toUpperCase();
        if (!upper.matches("[A-Z0-9_\\$]+")) {
            throw new IllegalArgumentException("Unsupported identifier format: " + ident);
        }
        return upper;
    }
}

