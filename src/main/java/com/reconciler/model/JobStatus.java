package com.reconciler.model;

/**
 * Immutable snapshot of the current reconciliation job state.
 * Shared between {@link com.reconciler.service.ReconcilerService} and
 * {@link com.reconciler.controller.ReconcilerController}.
 *
 * @param totalEstimate best-effort {@code COUNT(*)} of source + dest rows for
 *                       the table(s) currently in scope, captured once at the
 *                       start of the scan. {@code 0} means "unknown" (e.g. the
 *                       count query failed, or this status predates the feature) —
 *                       callers should treat that as "no percent available"
 *                       rather than literally 0%.
 */
public record JobStatus(
        String status,
        long   sourceCount,
        long   destCount,
        long   diffCount,
        String error,
        long   totalEstimate
) {
    /** Backward-compatible constructor for call sites that don't track a total estimate. */
    public JobStatus(String status, long sourceCount, long destCount, long diffCount, String error) {
        this(status, sourceCount, destCount, diffCount, error, 0);
    }

    public static JobStatus idle() {
        return new JobStatus("IDLE", 0, 0, 0, null, 0);
    }

    public static JobStatus running() {
        return new JobStatus("RUNNING", 0, 0, 0, null, 0);
    }

    /**
     * Percent of {@link #totalEstimate} processed so far ({@code sourceCount + destCount}),
     * capped at 100. Returns {@code -1} if no total estimate is available (unknown).
     */
    public int percentComplete() {
        if (totalEstimate <= 0) return -1;
        long done = sourceCount + destCount;
        int pct = (int) Math.min(100, Math.round((done * 100.0) / totalEstimate));
        return Math.max(0, pct);
    }
}

