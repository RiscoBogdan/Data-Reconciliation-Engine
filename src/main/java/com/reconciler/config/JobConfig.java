package com.reconciler.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Immutable DTO for a single reconciliation job request body.
 * Contains only table-level config — no credentials (those live in AppProperties).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record JobConfig(

        @JsonProperty("sourceTable")    String sourceTable,
        @JsonProperty("destTable")      String destTable,
        @JsonProperty("primaryKeys")    String primaryKeys,
        @JsonProperty("hashColumns")    List<String> hashColumns,
        @JsonProperty("modifiedColumn") String modifiedColumn,
        @JsonProperty("bufferMinutes")  Integer bufferMinutes,
        @JsonProperty("destFilterColumn") String destFilterColumn,
        @JsonProperty("destFilterValue")  String destFilterValue
) {
    /**
     * Delimiter used to join primary-key COLUMN NAMES in {@link #primaryKeys}.
     *
     * <p>Note this is safe (unlike the old {@code pk_composite} VALUE delimiter,
     * which was migrated to a control char): {@link #primaryKeys} holds column
     * <em>identifiers</em>, and every identifier is validated against
     * {@code [A-Z0-9_$.]} before it reaches SQL (see
     * {@code QueryStrategy.requireSafeIdentifier}), so a {@code '~'} can never
     * legitimately appear inside a key column name. The control-char delimiter
     * is reserved for composite key/hash VALUES, which can contain arbitrary data.
     */
    public static final String KEY_NAME_DELIMITER = "~";

    public List<String> keyColumns() {
        return List.of(primaryKeys.split(KEY_NAME_DELIMITER));
    }

    public int effectiveBufferMinutes() {
        return bufferMinutes != null ? bufferMinutes : 15;
    }

    public boolean hasDestFilter() {
        return destFilterColumn != null && !destFilterColumn.isBlank()
                && destFilterValue != null && !destFilterValue.isBlank();
    }

    @Override
    public String toString() {
        return "JobConfig[source=%s, dest=%s, keys=%s, buffer=%dm, destFilter=%s=%s]"
                .formatted(sourceTable, destTable, primaryKeys, effectiveBufferMinutes(),
                        destFilterColumn, destFilterValue);
    }
}
