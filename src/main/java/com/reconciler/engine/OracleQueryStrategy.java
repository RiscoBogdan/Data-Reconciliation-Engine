package com.reconciler.engine;

import com.reconciler.config.JobConfig;

import java.util.List;

/**
 * Oracle-dialect query builder.
 *
 * <h3>Normalization strategy</h3>
 * To stay symmetric with Snowflake and avoid value/type coercion surprises,
 * every hash column is converted with a simple deterministic cast:
 * {@code CAST(col AS VARCHAR2(4000))}.
 */
public final class OracleQueryStrategy implements QueryStrategy {

    @Override
    public String buildHashQuery(JobConfig config) {
        // Backward-compatible path if metadata is not supplied.
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
                : keyNames;

        // PK comparison key should be a direct cast-only composite string.
        String pkExpr = compositeKeyExpr(keyNames);

        String hashExpr = hashNames.stream()
                .map(h -> normalizeColumn(findColumn(columns, h)))
                .collect(java.util.stream.Collectors.joining(" || '" + HASH_DELIMITER + "' || "));

        String orderBy = String.join(", ", keyNames);

        var sb = new StringBuilder();
        sb.append("""
                SELECT
                    %s AS pk_composite,
                    RAWTOHEX(STANDARD_HASH(%s, 'MD5')) AS row_hash
                FROM %s
                """.formatted(pkExpr, hashExpr, QueryStrategy.requireSafeIdentifier(config.sourceTable())));

        List<String> predicates = new java.util.ArrayList<>();
        if (config.modifiedColumn() != null && !config.modifiedColumn().isBlank()) {
            predicates.add("%s < CURRENT_TIMESTAMP - INTERVAL '%d' MINUTE"
                    .formatted(QueryStrategy.requireSafeIdentifier(config.modifiedColumn()), config.effectiveBufferMinutes()));
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
        return "NVL(CAST(" + col + " AS VARCHAR2(4000)), 'NULL_STR')";
    }

    private String normalizeColumn(ColumnInfo col) {
        // Delegates to the shared HashNormalizer so Oracle and Snowflake stay
        // byte-for-byte identical (CODE_REVIEW C4).
        return HashNormalizer.oracle(col.name(), col.dataType());
    }
}
