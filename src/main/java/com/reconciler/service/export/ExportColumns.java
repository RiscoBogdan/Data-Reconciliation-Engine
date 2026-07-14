package com.reconciler.service.export;

/** Canonical column order + headers shared by every Difference Log exporter. */
public final class ExportColumns {

    private ExportColumns() {}

    /** Map keys (also used verbatim as XML element names). */
    public static final String[] HEADERS = {
            "id", "jobTs", "tableName", "pkComposite", "diffType",
            "sourceHash", "destHash", "changedColumns", "createdAt"
    };

    /** Human-friendly headers for the Excel data sheet. */
    public static final String[] EXCEL_TITLES = {
            "ID", "Job Timestamp", "Table", "Primary Key", "Diff Type",
            "Source Hash", "Dest Hash", "Changed Columns", "Detected At"
    };
}

