package com.reconciler.engine;

/**
 * Immutable value type representing a single row returned by either
 * the Oracle or Snowflake cursor.
 *
 * @param pkComposite Composite primary key string: "val1~val2~val3".
 *                    Built by the DB engine — never in Java memory.
 * @param rowHash     MD5 hex digest of all hashed columns.
 *                    Built by the DB engine — never in Java memory.
 */
public record Row(String pkComposite, String rowHash) {}

