package com.reconciler.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Request payload for running reconciliation across many tables.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BatchRunRequest(
        @JsonProperty("sourceSchema") String sourceSchema,
        @JsonProperty("sourceSchemas") List<String> sourceSchemas,
        @JsonProperty("destDatabase") String destDatabase,
        @JsonProperty("destSchema") String destSchema,
        @JsonProperty("schemaIds") List<String> schemaIds,
        @JsonProperty("tableConfigs") Map<String, TableConfigOverride> tableConfigs,
        @JsonProperty("tables") List<String> tables,
        @JsonProperty("modifiedColumn") String modifiedColumn,
        @JsonProperty("bufferMinutes") Integer bufferMinutes,
        @JsonProperty("clearBefore") Boolean clearBefore
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TableConfigOverride(
            @JsonProperty("primaryKeys") String primaryKeys,
            @JsonProperty("hashColumns") List<String> hashColumns,
            @JsonProperty("modifiedColumn") String modifiedColumn,
            @JsonProperty("bufferMinutes") Integer bufferMinutes
    ) {}

    public int effectiveBufferMinutes() {
        return bufferMinutes != null ? bufferMinutes : 15;
    }

    public boolean clearBeforeEffective() {
        return Boolean.TRUE.equals(clearBefore);
    }

    public boolean hasSchemaIds() {
        return schemaIds != null && schemaIds.stream().anyMatch(s -> s != null && !s.isBlank());
    }

    public boolean hasSourceSchemas() {
        return sourceSchemas != null && sourceSchemas.stream().anyMatch(s -> s != null && !s.isBlank());
    }

    public List<String> selectedSourceSchemas() {
        if (hasSourceSchemas()) {
            return sourceSchemas.stream()
                    .filter(s -> s != null && !s.isBlank())
                    .map(String::trim)
                    .distinct()
                    .toList();
        }
        if (sourceSchema != null && !sourceSchema.isBlank()) {
            return List.of(sourceSchema.trim());
        }
        return List.of();
    }

    public TableConfigOverride configForTable(String tableName) {
        if (tableConfigs == null || tableConfigs.isEmpty() || tableName == null || tableName.isBlank()) {
            return null;
        }
        String key = tableName.trim().toUpperCase(Locale.ROOT);
        TableConfigOverride direct = tableConfigs.get(key);
        if (direct != null) {
            return direct;
        }
        // Frontend may send schema-qualified keys (e.g. "SCHEMA.TABLE") to disambiguate
        // same-named tables across schemas. Fall back to matching by the bare table
        // name (portion after the last dot) so overrides are still applied.
        for (Map.Entry<String, TableConfigOverride> e : tableConfigs.entrySet()) {
            String k = e.getKey();
            int dot = k == null ? -1 : k.lastIndexOf('.');
            String tail = dot >= 0 ? k.substring(dot + 1) : k;
            if (key.equalsIgnoreCase(tail)) {
                return e.getValue();
            }
        }
        return null;
    }
}

