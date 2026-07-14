package com.reconciler.engine;

import java.util.Locale;

/**
 * Single source of truth for how each logical column type is normalized into a
 * canonical VARCHAR string <em>inside the database</em> before MD5 hashing.
 *
 * <p><b>Why this class exists (CODE_REVIEW C4):</b> the Oracle hash expression
 * and the Snowflake hash expression MUST produce byte-identical output for
 * semantically equal values, otherwise every row looks like a HASH_MISMATCH (or,
 * worse, real diffs are missed). Previously the two expressions lived in two
 * unrelated files ({@code OracleBatchIterator.normalizeForHash} and
 * {@code SnowflakeQueryStrategy.normalizeColumn}) with subtly different type
 * lists and DATE handling. Centralizing the rules here guarantees they stay in
 * lock-step and can be unit-tested against representative values.
 *
 * <p>Type classification is intentionally shared; only the dialect-specific
 * function names differ (e.g. {@code NVL}/{@code TO_CHAR} vs
 * {@code IFNULL}/{@code TO_CHAR}, {@code VARCHAR2} vs {@code VARCHAR}).
 */
public final class HashNormalizer {

    /** Canonical timestamp format used on BOTH sides — full date + time so a
     *  time-of-day-only difference in an Oracle {@code DATE} column is NOT
     *  silently dropped (previous bug: both sides truncated to date-only). */
    private static final String TS_FORMAT = "YYYY-MM-DD HH24:MI:SS";
    /** Canonical date-only format for pure DATE-without-time semantics. */
    private static final String DATE_FORMAT = "YYYY-MM-DD";

    private HashNormalizer() {}

    /** Logical classification shared by both dialects. */
    public enum Kind { NUMERIC, TIMESTAMP, DATE, TEXT }

    public static Kind classify(String rawType) {
        String t = rawType == null ? "" : rawType.toUpperCase(Locale.ROOT);
        if (t.contains("NUMBER") || t.contains("DECIMAL") || t.contains("NUMERIC")
                || t.contains("FLOAT") || t.contains("FIXED") || t.contains("REAL")
                || t.contains("DOUBLE") || t.contains("INT")) {
            return Kind.NUMERIC;
        }
        // TIMESTAMP must be checked before DATE ("TIMESTAMP" contains neither, but
        // some drivers report "TIMESTAMP WITH TIME ZONE" etc.).
        if (t.contains("TIMESTAMP")) return Kind.TIMESTAMP;
        // Oracle DATE carries a time component; treat it as a full timestamp so
        // time-of-day differences are captured. Snowflake pure DATE has no time,
        // but formatting it with a time mask yields "... 00:00:00" consistently.
        if (t.contains("DATE")) return Kind.TIMESTAMP;
        return Kind.TEXT;
    }

    /** Oracle-dialect normalized hash expression for one column. */
    public static String oracle(String c, String rawType) {
        return switch (classify(rawType)) {
            case NUMERIC -> "NVL(REGEXP_REPLACE(REGEXP_REPLACE(CAST(" + c
                    + " AS VARCHAR2(4000)), '(\\.\\d*?)0+$', '\\1'), '\\.$', ''), 'NULL_NUM')";
            case TIMESTAMP -> "NVL(TO_CHAR(" + c + ", '" + TS_FORMAT + "'), 'NULL_DATE')";
            case DATE -> "NVL(TO_CHAR(" + c + ", '" + DATE_FORMAT + "'), 'NULL_DATE')";
            case TEXT -> "NVL(CAST(" + c + " AS VARCHAR2(4000)), 'NULL_STR')";
        };
    }

    /** Snowflake-dialect normalized hash expression for one column. */
    public static String snowflake(String c, String rawType) {
        return switch (classify(rawType)) {
            case NUMERIC -> "IFNULL(REGEXP_REPLACE(REGEXP_REPLACE(CAST(" + c
                    + " AS VARCHAR(4000)), '(\\.\\d*?)0+$', '\\1'), '\\.$', ''), 'NULL_NUM')";
            case TIMESTAMP -> "IFNULL(TO_CHAR(" + c + ", '" + TS_FORMAT + "'), 'NULL_DATE')";
            case DATE -> "IFNULL(TO_CHAR(" + c + ", '" + DATE_FORMAT + "'), 'NULL_DATE')";
            case TEXT -> "IFNULL(CAST(" + c + " AS VARCHAR(4000)), 'NULL_STR')";
        };
    }
}

