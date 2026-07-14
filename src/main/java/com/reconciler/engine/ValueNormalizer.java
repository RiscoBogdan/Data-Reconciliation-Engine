package com.reconciler.engine;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;

/**
 * Canonical value normalization used for UI-level column diff display.
 *
 * <p>Rules (kept aligned with {@link HashNormalizer}, the SQL-side normalizer,
 * so the drill-down view doesn't flag a column "different" when the row hashes
 * actually matched — CODE_REVIEW L4):
 * <ul>
 *   <li>Numeric: strip trailing zeros and decimal point (100.00 -> 100)</li>
 *   <li>Date/Timestamp: normalize to yyyy-MM-dd HH:mm:ss (fractional seconds
 *       dropped — the SQL side likewise formats to second precision)</li>
 *   <li>Other: return trimmed value</li>
 * </ul>
 */
public final class ValueNormalizer {

    private static final DateTimeFormatter OUT_DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final List<DateTimeFormatter> DATE_TIME_FORMATTERS = List.of(
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("dd-MMM-yy hh.mm.ss")
                    .optionalStart().appendFraction(java.time.temporal.ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
                    .appendPattern(" a")
                    .toFormatter(Locale.ENGLISH)
    );

    private ValueNormalizer() {}

    public static String normalize(String value, String typeName) {
        if (value == null) return "";
        String v = value.trim();
        if (v.isEmpty()) return "";

        String t = typeName == null ? "" : typeName.toUpperCase(Locale.ROOT);
        if (isNumericType(t)) {
            try {
                BigDecimal bd = new BigDecimal(v);
                String out = bd.stripTrailingZeros().toPlainString();
                return out.equals("-0") ? "0" : out;
            } catch (Exception ignored) {
                return v;
            }
        }

        if (isDateType(t)) {
            LocalDateTime dt = tryParseDateTime(v);
            return dt != null ? dt.format(OUT_DATE_TIME) : v;
        }

        return v;
    }

    private static boolean isNumericType(String t) {
        return t.contains("NUMBER") || t.contains("DECIMAL") || t.contains("NUMERIC")
                || t.contains("FLOAT") || t.contains("REAL") || t.contains("FIXED")
                || t.contains("INTEGER") || t.contains("INT");
    }

    private static boolean isDateType(String t) {
        return t.contains("DATE") || t.contains("TIMESTAMP") || t.contains("TIME");
    }

    private static LocalDateTime tryParseDateTime(String v) {
        // direct yyyy-MM-dd
        try {
            return LocalDate.parse(v, DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay();
        } catch (DateTimeParseException ignored) {}

        // common date-time formats
        for (DateTimeFormatter f : DATE_TIME_FORMATTERS) {
            try {
                LocalDateTime dt = LocalDateTime.parse(v, f);
                return dt;
            } catch (DateTimeParseException ignored) {
            }
        }
        return null;
    }
}

