package com.reconciler.engine;

import com.reconciler.config.JobConfig;

import java.util.List;

/**
 * Snowflake-dialect query builder.
 *
 * <h3>Normalization strategy</h3>
 * For stability we intentionally avoid value-based coercion (e.g. TRY_TO_TIMESTAMP)
 * because VARCHAR values that happen to look like dates/numbers may be converted
 * differently between runs/session settings, causing noisy hash drift.
 *
 * <p>Each hash column is cast as VARCHAR deterministically.
 */
public final class SnowflakeQueryStrategy implements QueryStrategy {

    @Override
    public String buildHashQuery(JobConfig config) {
        List<ColumnInfo> cols = config.hashColumns() != null && !config.hashColumns().isEmpty()
                ? config.hashColumns().stream().map(c -> new ColumnInfo(c, "VARCHAR")).toList()
                : config.keyColumns().stream().map(c -> new ColumnInfo(c, "VARCHAR")).toList();
        return buildHashQuery(config, cols);
    }

    @Override
    public String buildHashQuery(JobConfig config, List<ColumnInfo> columns) {
        return buildHashQuery(config, columns, null);
    }

    @Override
    public String buildHashQuery(JobConfig config, List<ColumnInfo> columns, String resumeAfterPk) {
        List<String> keyNames = config.keyColumns();
        List<String> hashNames = config.hashColumns() != null && !config.hashColumns().isEmpty()
                ? config.hashColumns()
                : keyNames; // keep behavior symmetric with Oracle iterator fallback

        // Composite PK must use raw casts (not numeric/date masks), otherwise
        // values like 1 become "1." and break sort-merge key alignment.
        String pkExpr = compositeKeyExpr(keyNames);

        String hashExpr = hashNames.stream()
                .map(h -> normalizeColumn(findColumn(columns, h)))
                .collect(java.util.stream.Collectors.joining(" || '" + HASH_DELIMITER + "' || "));

        String orderBy = String.join(", ", keyNames);

        var sb = new StringBuilder();
        sb.append("""
                SELECT
                    %s AS pk_composite,
                    MD5(%s) AS row_hash
                FROM %s
                """.formatted(pkExpr, hashExpr, QueryStrategy.requireSafeIdentifier(config.destTable())));

        List<String> predicates = new java.util.ArrayList<>();
        if (config.modifiedColumn() != null && !config.modifiedColumn().isBlank()) {
            predicates.add("%s < DATEADD('MINUTE', -%d, CURRENT_TIMESTAMP())"
                    .formatted(QueryStrategy.requireSafeIdentifier(config.modifiedColumn()), config.effectiveBufferMinutes()));
        }
        if (config.hasDestFilter()) {
            predicates.add("%s = '%s'"
                    .formatted(QueryStrategy.requireSafeIdentifier(config.destFilterColumn()), safeLiteral(config.destFilterValue())));
        }
        if (resumeAfterPk != null && !resumeAfterPk.isBlank()) {
            // Type-aware comparison (numeric keys compare numerically, not as
            // lexicographic strings) — see QueryStrategy#resumeCondition.
            predicates.add(resumeCondition(keyNames, columns, resumeAfterPk));
        }
        if (!predicates.isEmpty()) {
            sb.append("WHERE ").append(String.join(" AND ", predicates)).append("\n");
        }

        sb.append("ORDER BY ").append(orderBy);
        return sb.toString().trim();
    }

    @Override
    public String normalizeColumn(String col) {
        return "IFNULL(CAST(" + col + " AS VARCHAR(4000)), 'NULL_STR')";
    }

    private String normalizeColumn(ColumnInfo col) {
        // Delegates to the shared HashNormalizer so Oracle and Snowflake stay
        // byte-for-byte identical (CODE_REVIEW C4).
        return HashNormalizer.snowflake(col.name(), col.dataType());
    }


    private String safeLiteral(String value) {
        return value == null ? "" : value.replace("'", "''");
    }
}
