package com.reconciler.engine;

import com.reconciler.config.JobConfig;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Strategy interface for generating database-specific SQL queries.
 *
 * <h3>Hash normalization</h3>
 * <p>The core challenge: {@code CAST(value AS VARCHAR)} produces different strings
 * for the same value across databases:
 * <ul>
 *   <li>Oracle TIMESTAMP → {@code "10-JUL-26 05.00.00.000000 PM"} (NLS-dependent)</li>
 *   <li>Snowflake TIMESTAMP → {@code "2026-07-10 17:00:00.000000000"}</li>
 *   <li>Oracle NUMBER 100 → {@code "100"}, Snowflake → {@code "100"} (fine)</li>
 *   <li>Oracle NUMBER 1234567890.12 → {@code "1234567890.12"} vs Snowflake scientific notation</li>
 * </ul>
 *
 * <p>Each dialect implementation overrides {@link #normalizeColumn(String)} to
 * apply database-specific normalization so both sides produce identical strings
 * before the MD5 is computed.
 */
public sealed interface QueryStrategy permits OracleQueryStrategy, SnowflakeQueryStrategy {

    /** Lightweight column metadata used for type-aware normalization. */
    record ColumnInfo(String name, String dataType) {}

    /**
     * Delimiter used to join composite-PK column parts into a single
     * {@code pk_composite} string (and to split it back apart for resume).
     *
     * <p>Uses the ASCII "Unit Separator" control character (0x1F) instead of
     * a printable character like {@code '~'}. {@code '~'} can legitimately
     * appear inside a real string PK column's value (e.g. {@code "2~ABC"}),
     * which would corrupt the split when reconstructing per-column values for
     * a resumed/paginated scan. {@code \u001F} is a non-printable control
     * character reserved exactly for this purpose and is for all practical
     * purposes never present in real business data.
     *
     * <p>Both Oracle and Snowflake must use this SAME delimiter when building
     * {@code pk_composite}, since {@link CompareEngine}'s sort-merge zipper
     * compares the two sides' composite strings directly.
     */
    String PK_DELIMITER = "\u001F";

    /** Legacy delimiter used before {@link #PK_DELIMITER} — kept only so old,
     *  already-persisted resume checkpoints (from before this fix) can still
     *  be parsed once, as a fallback. New jobs always use {@link #PK_DELIMITER}. */
    String LEGACY_PK_DELIMITER = "~";

    /**
     * Delimiter used to join per-column normalized values into the single
     * string that gets MD5-hashed on BOTH sides. Uses the ASCII "Record
     * Separator" control char (0x1E) — for the same collision-avoidance reason
     * as {@link #PK_DELIMITER} (see C3 in CODE_REVIEW): a printable delimiter
     * like {@code '~'} or {@code '|'} can legitimately appear inside a column
     * value, letting {@code ["a~","b"]} and {@code ["a","~b"]} hash identically.
     *
     * <p>MUST be identical in {@link OracleQueryStrategy}, {@link OracleBatchIterator}
     * and {@link SnowflakeQueryStrategy}, otherwise the two sides compute
     * different hashes for identical rows and every row looks like a mismatch.
     */
    String HASH_DELIMITER = "\u001E";

    /**
     * Validates a SQL identifier (or schema-qualified {@code SCHEMA.TABLE} /
     * {@code DB.SCHEMA.TABLE}) against a strict allow-list so it can be safely
     * concatenated into SQL text. Rejects anything containing quotes, spaces,
     * semicolons, comment markers, etc. Returns the trimmed, upper-cased value.
     *
     * <p>{@code static} so every builder (both strategies + the Oracle iterator)
     * shares the exact same guard instead of each re-implementing it (see S2).
     *
     * @throws IllegalArgumentException if {@code ident} is null/blank or contains
     *                                  any character outside {@code [A-Z0-9_$.]}.
     */
    static String requireSafeIdentifier(String ident) {
        String upper = ident == null ? "" : ident.trim().toUpperCase(Locale.ROOT);
        if (upper.isBlank() || !upper.matches("[A-Z0-9_$]+(\\.[A-Z0-9_$]+)*")) {
            throw new IllegalArgumentException("Unsupported / unsafe SQL identifier: " + ident);
        }
        return upper;
    }

    String buildHashQuery(JobConfig config);

    /**
     * Type-aware query generation; default falls back to legacy implementation.
     */
    default String buildHashQuery(JobConfig config, List<ColumnInfo> columns) {
        return buildHashQuery(config);
    }

    /**
     * Resumable variant: adds {@code WHERE <pk_composite_expr> > <resumeAfterPk>}
     * so a truncated/interrupted scan can restart past the last checkpointed key
     * instead of re-scanning the whole table. {@code resumeAfterPk} is the exact
     * {@code pk_composite} string value of the last row that was fully processed
     * (matched/diffed) before the interruption — {@code null} means "from the start".
     *
     * <p>This is safe precisely because the sort-merge zipper in
     * {@link CompareEngine} already treats {@code pk_composite} (a same-format
     * CAST-to-string composite key on both sides) as the ordering key for its
     * merge comparisons — so filtering on that same string expression can't
     * introduce any inconsistency that the merge itself doesn't already rely on.
     */
    default String buildHashQuery(JobConfig config, List<ColumnInfo> columns, String resumeAfterPk) {
        return buildHashQuery(config, columns);
    }

    // ── Shared builders ───────────────────────────────────────────────────

    /** Builds: {@code CAST(pk1 AS VARCHAR(4000)) || <PK_DELIMITER> || CAST(pk2 ...)} */
    default String compositeKeyExpr(List<String> keys) {
        String sep = "'" + PK_DELIMITER + "'";
        return keys.stream()
                .map(k -> "CAST(" + k + " AS VARCHAR(4000))")
                .collect(Collectors.joining(" || " + sep + " || "));
    }

    /**
     * Builds the normalized hash-input string for a list of columns.
     * Each column is passed through {@link #normalizeColumn(String)} before
     * being wrapped in {@code COALESCE(..., '')} and concatenated.
     */
    default String hashInputExpr(List<String> cols) {
        return cols.stream()
                .map(c -> "COALESCE(" + normalizeColumn(c) + ", '')")
                .collect(Collectors.joining(" || '" + HASH_DELIMITER + "' || "));
    }

    /**
     * Converts a single column to a normalized, comparable VARCHAR string.
     *
     * <p>The default implementation simply casts to VARCHAR — suitable for
     * string columns.  Dialect implementations override this to handle
     * timestamps and numbers consistently across databases.
     *
     * <p>Implementations must produce the <em>same output string</em> for
     * semantically equal values in Oracle and Snowflake.
     */
    default String normalizeColumn(String col) {
        return "CAST(" + col + " AS VARCHAR(4000))";
    }

    /** Finds a column metadata entry by name (case-insensitive). */
    default ColumnInfo findColumn(List<ColumnInfo> columns, String name) {
        String needle = name == null ? "" : name.toUpperCase(Locale.ROOT);
        return columns.stream()
                .filter(c -> c.name() != null && c.name().toUpperCase(Locale.ROOT).equals(needle))
                .findFirst()
                .orElse(new ColumnInfo(name, "VARCHAR"));
    }

    /** {@code true} for any numeric SQL type name (NUMBER/DECIMAL/NUMERIC/FLOAT/INT/REAL/...). */
    default boolean isNumericType(String dataType) {
        if (dataType == null) return false;
        String t = dataType.toUpperCase(Locale.ROOT);
        return t.contains("NUMBER") || t.contains("DECIMAL") || t.contains("NUMERIC")
                || t.contains("FLOAT") || t.contains("INT") || t.contains("REAL")
                || t.contains("DOUBLE");
    }

    /**
     * Builds a type-aware keyset "resume after" predicate.
     *
     * <p><b>Why this matters:</b> {@code resumeAfterPk} is the composite
     * {@code pk_composite} string (columns joined with {@link #PK_DELIMITER}).
     * Comparing that whole string with {@code >} works for text/string keys,
     * but is <em>wrong</em> for numeric keys — e.g. {@code '250' > '1500'} is
     * {@code true} lexicographically even though {@code 250 < 1500}
     * numerically. That mismatch caused resumed/paginated scans to skip or
     * duplicate rows whenever the primary key was numeric.
     *
     * <p>This splits the composite value back into its per-column parts and
     * compares each part using its <em>actual</em> column type: numeric
     * columns get an unquoted numeric literal (so Oracle/Snowflake compare
     * numerically), everything else keeps the quoted string comparison.
     * For composite keys, a row-value constructor
     * {@code (col1, col2) > (val1, val2)} is used so ordering matches the
     * {@code ORDER BY col1, col2} clause exactly.
     *
     * <p>Splitting uses {@link #PK_DELIMITER} (a control character), NOT a
     * printable separator — a string PK column's actual value could
     * legitimately contain something like {@code "2~ABC"}, and splitting on
     * a printable {@code '~'} would misalign every column after it.
     */
    default String resumeCondition(List<String> keyNames, List<ColumnInfo> columns, String resumeAfterPk) {
        String[] parts = splitComposite(resumeAfterPk, keyNames.size());
        if (parts == null) {
            // Couldn't cleanly split into exactly one part per key column
            // (extremely unlikely, but possible for legacy checkpoints
            // written before PK_DELIMITER existed, if a value also contains
            // '~'). Fall back to a plain composite-string comparison rather
            // than risk misaligned/garbage bind values.
            return compositeKeyExpr(keyNames) + " > '" + resumeAfterPk.replace("'", "''") + "'";
        }

        List<String> colExprs = new java.util.ArrayList<>(keyNames.size());
        List<String> literalExprs = new java.util.ArrayList<>(keyNames.size());
        for (int i = 0; i < keyNames.size(); i++) {
            String col = keyNames.get(i);
            String val = parts[i];
            boolean numeric = isNumericType(findColumn(columns, col).dataType());
            colExprs.add(col);
            if (numeric && !val.isBlank()) {
                literalExprs.add(val); // unquoted numeric literal — numeric comparison
            } else {
                literalExprs.add("'" + val.replace("'", "''") + "'");
            }
        }

        if (colExprs.size() == 1) {
            return colExprs.get(0) + " > " + literalExprs.get(0);
        }
        return "(" + String.join(", ", colExprs) + ") > (" + String.join(", ", literalExprs) + ")";
    }

    /**
     * Splits a composite key string into exactly {@code expectedParts} pieces.
     * Tries {@link #PK_DELIMITER} first (current format); if that doesn't
     * produce the expected count (e.g. an older checkpoint persisted before
     * this fix), falls back to {@link #LEGACY_PK_DELIMITER}. Returns
     * {@code null} if neither produces the expected number of parts.
     *
     * <p>{@code static} (not {@code default}) so callers that don't have a
     * {@link QueryStrategy} instance handy — e.g. {@link OracleBatchIterator},
     * which builds its own SQL independently for performance reasons — can
     * still share this exact same splitting logic instead of duplicating it.
     */
    static String[] splitComposite(String composite, int expectedParts) {
        String[] parts = composite.split(PK_DELIMITER, -1);
        if (parts.length == expectedParts) return parts;
        String[] legacy = composite.split(LEGACY_PK_DELIMITER, -1);
        if (legacy.length == expectedParts) return legacy;
        return null;
    }
}
