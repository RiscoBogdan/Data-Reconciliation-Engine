package com.reconciler.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Spring Boot-bound configuration for all database connections.
 *
 * <p>Values come from {@code application.yml} under the {@code reconciler} prefix,
 * with transparent fallback to environment variables via Spring's
 * {@code ${ENV_VAR:default}} placeholder syntax.
 *
 * <p>Example YAML:
 * <pre>{@code
 * reconciler:
 *   oracle:
 *     jdbc-url: ${ORACLE_JDBC_URL:jdbc:oracle:thin:@//localhost:1521/FREEPDB1}
 *     user: ${ORACLE_USER:RECON}
 *     password: ${ORACLE_PASSWORD:recon}
 *   snowflake:
 *     jdbc-url: ${SNOWFLAKE_JDBC_URL:jdbc:snowflake://...}
 *     user: ${SNOWFLAKE_USER:BRISCO}
 *     password: ${SNOWFLAKE_PASSWORD:secret}
 *   sqlite:
 *     path: ${SQLITE_PATH:reconciler_diffs.db}
 * }</pre>
 */
@ConfigurationProperties(prefix = "reconciler")
public class AppProperties {

    private final Oracle oracle;
    private final Snowflake snowflake;
    private final Sqlite sqlite;
    private final List<SchemaMapping> mappings;

    public AppProperties(Oracle oracle, Snowflake snowflake, Sqlite sqlite, List<SchemaMapping> mappings) {
        this.oracle    = oracle    != null ? oracle    : new Oracle(null, null, null);
        this.snowflake = snowflake != null ? snowflake : new Snowflake(null, null, null);
        this.sqlite    = sqlite    != null ? sqlite    : new Sqlite("reconciler_diffs.db");
        this.mappings  = mappings  != null ? List.copyOf(mappings) : List.of();
    }

    public Oracle    oracle()    { return oracle; }
    public Snowflake snowflake() { return snowflake; }
    public Sqlite    sqlite()    { return sqlite; }
    public List<SchemaMapping> mappings() { return mappings; }

    public List<SchemaMapping> enabledMappings() {
        return mappings.stream().filter(SchemaMapping::enabled).toList();
    }

    public Map<String, SchemaMapping> mappingById() {
        return mappings.stream()
                .filter(m -> m.schemaId() != null && !m.schemaId().isBlank())
                .collect(Collectors.toMap(
                        m -> m.schemaId().trim().toUpperCase(Locale.ROOT),
                        Function.identity(),
                        (a, b) -> a
                ));
    }

    public record Oracle(String jdbcUrl, String user, String password) {}
    public record Snowflake(String jdbcUrl, String user, String password) {}
    public record Sqlite(String path) {}
    public record SchemaMapping(String schemaId,
                                String oracleSchema,
                                String snowflakeDatabase,
                                String snowflakeSchema,
                                String destFilterColumn,
                                String destFilterValue,
                                boolean enabled) {
        public SchemaMapping {
            schemaId = schemaId == null ? "" : schemaId.trim();
            oracleSchema = oracleSchema == null ? "" : oracleSchema.trim();
            snowflakeDatabase = snowflakeDatabase == null ? "" : snowflakeDatabase.trim();
            snowflakeSchema = snowflakeSchema == null ? "" : snowflakeSchema.trim();
            destFilterColumn = destFilterColumn == null ? "" : destFilterColumn.trim();
            destFilterValue = destFilterValue == null ? "" : destFilterValue.trim();
        }

        public String normalizedSchemaId() {
            return schemaId.toUpperCase(Locale.ROOT);
        }

        public boolean isUsable() {
            return !schemaId.isBlank() && !oracleSchema.isBlank() && !snowflakeSchema.isBlank();
        }

        public boolean hasDestFilter() {
            return !destFilterColumn.isBlank() && !destFilterValue.isBlank();
        }
    }
}

