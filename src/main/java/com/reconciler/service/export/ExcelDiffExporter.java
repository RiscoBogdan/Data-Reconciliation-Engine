package com.reconciler.service.export;

import com.reconciler.repository.DiffRepository;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xddf.usermodel.PresetColor;
import org.apache.poi.xddf.usermodel.XDDFColor;
import org.apache.poi.xddf.usermodel.XDDFSolidFillProperties;
import org.apache.poi.xddf.usermodel.chart.AxisCrosses;
import org.apache.poi.xddf.usermodel.chart.AxisPosition;
import org.apache.poi.xddf.usermodel.chart.BarDirection;
import org.apache.poi.xddf.usermodel.chart.ChartTypes;
import org.apache.poi.xddf.usermodel.chart.LegendPosition;
import org.apache.poi.xddf.usermodel.chart.XDDFBarChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFCategoryAxis;
import org.apache.poi.xddf.usermodel.chart.XDDFChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFChartLegend;
import org.apache.poi.xddf.usermodel.chart.XDDFDataSource;
import org.apache.poi.xddf.usermodel.chart.XDDFDataSourcesFactory;
import org.apache.poi.xddf.usermodel.chart.XDDFNumericalDataSource;
import org.apache.poi.xddf.usermodel.chart.XDDFPieChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFValueAxis;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFChart;
import org.apache.poi.xssf.usermodel.XSSFClientAnchor;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFDrawing;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Component;

import java.awt.Color;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * Produces a polished, chart-rich Excel (.xlsx) export of the Difference Log.
 *
 * <p>Two sheets:
 * <ul>
 *   <li><b>Summary</b> — headline totals plus a pie chart (diff-type breakdown)
 *       and a bar chart (per-table diff counts). Built on a real XSSF sheet so
 *       charts are supported.</li>
 *   <li><b>Differences</b> — the full filtered data set, streamed via SXSSF so
 *       millions of rows stay memory-flat. Styled header, banded rows, colored
 *       diff-type cells, frozen header row and an auto-filter.</li>
 * </ul>
 */
@Component
public class ExcelDiffExporter {

    /** Below Excel's hard limit of 1,048,576 rows; overflow spills to new sheets. */
    private static final int MAX_ROWS_PER_SHEET = 1_000_000;

    private final DiffRepository diffRepo;

    public ExcelDiffExporter(DiffRepository diffRepo) {
        this.diffRepo = diffRepo;
    }

    public void write(OutputStream out, String jobTs, String search, String table,
                      DiffRepository.Filters filters) throws Exception {
        // Keep only 200 rows in memory at a time; the rest are flushed to disk.
        try (SXSSFWorkbook wb = new SXSSFWorkbook(200)) {
            wb.setCompressTempFiles(true);

            List<Map<String, Object>> summaries = jobTs != null && !jobTs.isBlank()
                    ? diffRepo.tableSummariesByJobTs(null, jobTs)
                    : diffRepo.tableSummaries(null);

            Styles styles = new Styles(wb);
            buildSummarySheet(wb, summaries, jobTs);
            long total = buildDataSheet(wb, styles, jobTs, search, table, filters);

            // If the data sheet ended up empty, still leave a friendly note.
            if (total == 0) {
                SXSSFSheet sheet = wb.getSheet("Differences");
                Row r = sheet.createRow(1);
                r.createCell(0).setCellValue("No differences matched the current filters.");
            }

            wb.write(out);
            wb.dispose();
        }
    }

    // ── Summary sheet (charts) ───────────────────────────────────────────

    private void buildSummarySheet(SXSSFWorkbook wb, List<Map<String, Object>> summaries, String jobTs) {
        // Charts require a genuine XSSF sheet, so build the summary on the
        // underlying XSSFWorkbook and let SXSSF wrap it.
        XSSFWorkbook xssf = wb.getXSSFWorkbook();
        XSSFSheet sheet = xssf.createSheet("Summary");
        sheet.setColumnWidth(0, 26 * 256);
        sheet.setColumnWidth(1, 16 * 256);
        sheet.setColumnWidth(2, 16 * 256);
        sheet.setColumnWidth(3, 16 * 256);

        Styles s = new Styles(wb);

        Row title = sheet.createRow(0);
        Cell titleCell = title.createCell(0);
        titleCell.setCellValue("Reconciliation Difference Report");
        titleCell.setCellStyle(s.title);
        sheet.addMergedRegion(new CellRangeAddress(0, 0, 0, 3));

        Row scopeRow = sheet.createRow(1);
        scopeRow.createCell(0).setCellValue("Scope");
        scopeRow.createCell(1).setCellValue(jobTs != null && !jobTs.isBlank()
                ? "Latest job (" + jobTs + ")" : "All history");

        // Aggregate diff-type totals across all tables.
        long hashMismatch = 0, missingDest = 0, missingSrc = 0, grand = 0;
        for (Map<String, Object> m : summaries) {
            hashMismatch += toLong(m.get("hashMismatch"));
            missingDest  += toLong(m.get("missingInDest"));
            missingSrc   += toLong(m.get("missingInSource"));
            grand        += toLong(m.get("total"));
        }

        // Headline totals block.
        int r = 3;
        writeKeyValue(sheet, r++, "Total Differences", grand, s);
        writeKeyValue(sheet, r++, "Tables With Differences", summaries.size(), s);

        // Diff-type breakdown table (used as the pie chart's data source).
        int pieHeaderRow = ++r;
        Row h = sheet.createRow(pieHeaderRow);
        styleHeaderCell(h.createCell(0), "Diff Type", s);
        styleHeaderCell(h.createCell(1), "Count", s);
        String[] types = {"HASH_MISMATCH", "MISSING_IN_DEST", "MISSING_IN_SOURCE"};
        long[] counts = {hashMismatch, missingDest, missingSrc};
        int pieFirstDataRow = pieHeaderRow + 1;
        for (int i = 0; i < types.length; i++) {
            Row dr = sheet.createRow(pieFirstDataRow + i);
            dr.createCell(0).setCellValue(types[i]);
            dr.createCell(1).setCellValue(counts[i]);
        }
        int pieLastDataRow = pieFirstDataRow + types.length - 1;

        drawPieChart(sheet, pieFirstDataRow, pieLastDataRow);

        // Per-table breakdown table (bar chart data source) placed below the charts.
        int tblHeaderRow = pieLastDataRow + 18;
        Row th = sheet.createRow(tblHeaderRow);
        styleHeaderCell(th.createCell(0), "Table", s);
        styleHeaderCell(th.createCell(1), "Total", s);
        styleHeaderCell(th.createCell(2), "Missing in Dest", s);
        styleHeaderCell(th.createCell(3), "Missing in Source", s);
        int tblFirstDataRow = tblHeaderRow + 1;
        int i = 0;
        for (Map<String, Object> m : summaries) {
            Row dr = sheet.createRow(tblFirstDataRow + i);
            dr.createCell(0).setCellValue(String.valueOf(m.getOrDefault("tableName", "")));
            dr.createCell(1).setCellValue(toLong(m.get("total")));
            dr.createCell(2).setCellValue(toLong(m.get("missingInDest")));
            dr.createCell(3).setCellValue(toLong(m.get("missingInSource")));
            i++;
        }
        int tblLastDataRow = tblFirstDataRow + Math.max(0, summaries.size() - 1);
        if (!summaries.isEmpty()) {
            drawBarChart(sheet, tblFirstDataRow, tblLastDataRow);
        }
    }

    private void drawPieChart(XSSFSheet sheet, int firstDataRow, int lastDataRow) {
        XSSFDrawing drawing = sheet.createDrawingPatriarch();
        // Anchor to the right of the data (cols 5..12, rows firstDataRow-1 .. +15).
        XSSFClientAnchor anchor = drawing.createAnchor(
                0, 0, 0, 0, 5, firstDataRow - 1, 12, firstDataRow + 14);
        XSSFChart chart = drawing.createChart(anchor);
        chart.setTitleText("Differences by Type");
        chart.setTitleOverlay(false);
        XDDFChartLegend legend = chart.getOrAddLegend();
        legend.setPosition(LegendPosition.RIGHT);

        XDDFDataSource<String> cats = XDDFDataSourcesFactory.fromStringCellRange(
                sheet, new CellRangeAddress(firstDataRow, lastDataRow, 0, 0));
        XDDFNumericalDataSource<Double> vals = XDDFDataSourcesFactory.fromNumericCellRange(
                sheet, new CellRangeAddress(firstDataRow, lastDataRow, 1, 1));

        XDDFPieChartData data = (XDDFPieChartData) chart.createData(ChartTypes.PIE, null, null);
        data.setVaryColors(true);
        data.addSeries(cats, vals);
        chart.plot(data);
    }

    private void drawBarChart(XSSFSheet sheet, int firstDataRow, int lastDataRow) {
        XSSFDrawing drawing = sheet.createDrawingPatriarch();
        XSSFClientAnchor anchor = drawing.createAnchor(
                0, 0, 0, 0, 5, firstDataRow - 1, 14, firstDataRow + 15);
        XSSFChart chart = drawing.createChart(anchor);
        chart.setTitleText("Differences by Table");
        chart.setTitleOverlay(false);
        XDDFChartLegend legend = chart.getOrAddLegend();
        legend.setPosition(LegendPosition.TOP);

        XDDFCategoryAxis catAx = chart.createCategoryAxis(AxisPosition.BOTTOM);
        XDDFValueAxis valAx = chart.createValueAxis(AxisPosition.LEFT);
        valAx.setCrosses(AxisCrosses.AUTO_ZERO);

        XDDFDataSource<String> cats = XDDFDataSourcesFactory.fromStringCellRange(
                sheet, new CellRangeAddress(firstDataRow, lastDataRow, 0, 0));
        XDDFNumericalDataSource<Double> totals = XDDFDataSourcesFactory.fromNumericCellRange(
                sheet, new CellRangeAddress(firstDataRow, lastDataRow, 1, 1));

        XDDFBarChartData bar = (XDDFBarChartData) chart.createData(ChartTypes.BAR, catAx, valAx);
        bar.setBarDirection(BarDirection.COL);
        XDDFChartData.Series series = bar.addSeries(cats, totals);
        series.setTitle("Total", null);
        series.setFillProperties(new XDDFSolidFillProperties(
                XDDFColor.from(PresetColor.STEEL_BLUE)));
        chart.plot(bar);
    }

    // ── Data sheet (streamed) ────────────────────────────────────────────

    private long buildDataSheet(SXSSFWorkbook wb, Styles styles, String jobTs,
                                String search, String table, DiffRepository.Filters filters) {
        SXSSFSheet[] sheet = { wb.createSheet("Differences") };
        sheet[0].trackAllColumnsForAutoSizing();
        int[] sheetIdx = { 0 };
        long[] rowNum = { 0 };
        writeHeaderRow(sheet[0], styles);
        freezeAndFilter(sheet[0]);

        diffRepo.streamFiltered(jobTs, search, table, filters, rec -> {
            if (rowNum[0] > 0 && rowNum[0] % MAX_ROWS_PER_SHEET == 0) {
                sheetIdx[0]++;
                sheet[0] = wb.createSheet("Differences_" + sheetIdx[0]);
                writeHeaderRow(sheet[0], styles);
                freezeAndFilter(sheet[0]);
            }
            int excelRow = (int) (rowNum[0] % MAX_ROWS_PER_SHEET) + 1;
            Row row = sheet[0].createRow(excelRow);
            boolean banded = (rowNum[0] & 1L) == 0L;
            for (int c = 0; c < ExportColumns.HEADERS.length; c++) {
                Cell cell = row.createCell(c);
                cell.setCellValue(String.valueOf(rec.getOrDefault(ExportColumns.HEADERS[c], "")));
                cell.setCellStyle(banded ? styles.bodyBanded : styles.body);
            }
            // Recolor the Diff Type cell (index 4) as a badge.
            String diffType = String.valueOf(rec.getOrDefault("diffType", ""));
            Cell typeCell = row.getCell(4);
            typeCell.setCellStyle(styles.forDiffType(diffType, banded));
            rowNum[0]++;
        });

        // Fixed widths (cheaper + more predictable than autosizing millions of rows).
        int[] widths = {8, 22, 18, 40, 20, 26, 26, 30, 22};
        for (int c = 0; c < widths.length; c++) {
            sheet[0].setColumnWidth(c, widths[c] * 256);
        }
        return rowNum[0];
    }

    private void writeHeaderRow(SXSSFSheet sheet, Styles styles) {
        Row header = sheet.createRow(0);
        for (int c = 0; c < ExportColumns.EXCEL_TITLES.length; c++) {
            Cell cell = header.createCell(c);
            cell.setCellValue(ExportColumns.EXCEL_TITLES[c]);
            cell.setCellStyle(styles.header);
        }
    }

    private void freezeAndFilter(SXSSFSheet sheet) {
        sheet.createFreezePane(0, 1);
        sheet.setAutoFilter(new CellRangeAddress(0, 0, 0, ExportColumns.HEADERS.length - 1));
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private void writeKeyValue(XSSFSheet sheet, int rowIdx, String key, long value, Styles s) {
        Row row = sheet.createRow(rowIdx);
        Cell k = row.createCell(0);
        k.setCellValue(key);
        k.setCellStyle(s.keyLabel);
        row.createCell(1).setCellValue(value);
    }

    private void styleHeaderCell(Cell cell, String text, Styles s) {
        cell.setCellValue(text);
        cell.setCellStyle(s.header);
    }

    private static long toLong(Object o) {
        return o instanceof Number n ? n.longValue() : 0L;
    }

    /** Reusable cell styles + fonts for one workbook. */
    private static final class Styles {
        final CellStyle title;
        final CellStyle header;
        final CellStyle keyLabel;
        final CellStyle body;
        final CellStyle bodyBanded;
        final CellStyle badgeAmber;
        final CellStyle badgeAmberBanded;
        final CellStyle badgeRed;
        final CellStyle badgeRedBanded;
        final CellStyle badgeBlue;
        final CellStyle badgeBlueBanded;

        Styles(SXSSFWorkbook wb) {
            XSSFWorkbook xssf = wb.getXSSFWorkbook();

            Font titleFont = wb.createFont();
            titleFont.setBold(true);
            titleFont.setFontHeightInPoints((short) 16);
            title = wb.createCellStyle();
            title.setFont(titleFont);

            Font headerFont = wb.createFont();
            headerFont.setBold(true);
            headerFont.setColor(org.apache.poi.ss.usermodel.IndexedColors.WHITE.getIndex());
            org.apache.poi.xssf.usermodel.XSSFCellStyle hs = xssf.createCellStyle();
            hs.setFont(headerFont);
            hs.setFillForegroundColor(new XSSFColor(new Color(30, 58, 95), null));
            hs.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            hs.setAlignment(HorizontalAlignment.CENTER);
            hs.setBorderBottom(BorderStyle.THIN);
            header = hs;

            Font keyFont = wb.createFont();
            keyFont.setBold(true);
            keyLabel = wb.createCellStyle();
            keyLabel.setFont(keyFont);

            body = base(xssf, null);
            bodyBanded = base(xssf, new Color(238, 243, 250));

            badgeAmber       = badge(xssf, new Color(255, 244, 214), new Color(146, 96, 0));
            badgeAmberBanded = badge(xssf, new Color(255, 236, 189), new Color(146, 96, 0));
            badgeRed         = badge(xssf, new Color(253, 226, 226), new Color(155, 28, 28));
            badgeRedBanded   = badge(xssf, new Color(250, 210, 210), new Color(155, 28, 28));
            badgeBlue        = badge(xssf, new Color(219, 234, 254), new Color(30, 64, 155));
            badgeBlueBanded  = badge(xssf, new Color(200, 222, 252), new Color(30, 64, 155));
        }

        private static org.apache.poi.xssf.usermodel.XSSFCellStyle base(XSSFWorkbook xssf, Color fill) {
            org.apache.poi.xssf.usermodel.XSSFCellStyle st = xssf.createCellStyle();
            if (fill != null) {
                st.setFillForegroundColor(new XSSFColor(fill, null));
                st.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            }
            return st;
        }

        private static org.apache.poi.xssf.usermodel.XSSFCellStyle badge(
                XSSFWorkbook xssf, Color fill, Color text) {
            org.apache.poi.xssf.usermodel.XSSFCellStyle st = xssf.createCellStyle();
            st.setFillForegroundColor(new XSSFColor(fill, null));
            st.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            st.setAlignment(HorizontalAlignment.CENTER);
            org.apache.poi.xssf.usermodel.XSSFFont f = xssf.createFont();
            f.setBold(true);
            f.setColor(new XSSFColor(text, null));
            st.setFont(f);
            return st;
        }

        CellStyle forDiffType(String type, boolean banded) {
            return switch (type) {
                case "HASH_MISMATCH"     -> banded ? badgeAmberBanded : badgeAmber;
                case "MISSING_IN_DEST"   -> banded ? badgeRedBanded : badgeRed;
                case "MISSING_IN_SOURCE" -> banded ? badgeBlueBanded : badgeBlue;
                default                  -> banded ? bodyBanded : body;
            };
        }
    }
}

