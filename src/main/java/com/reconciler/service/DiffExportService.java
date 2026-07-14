package com.reconciler.service;

import com.reconciler.repository.DiffRepository;
import com.reconciler.service.export.ExcelDiffExporter;
import com.reconciler.service.export.XmlDiffExporter;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Orchestrates Difference Log exports: picks the right format-specific exporter,
 * sets the HTTP content-type / download filename, and delegates the streaming.
 * Keeping this out of the controller keeps the web layer thin and the export
 * logic independently testable.
 */
@Service
public class DiffExportService {

    private static final DateTimeFormatter STAMP = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    private final ExcelDiffExporter excelExporter;
    private final XmlDiffExporter xmlExporter;

    public DiffExportService(ExcelDiffExporter excelExporter, XmlDiffExporter xmlExporter) {
        this.excelExporter = excelExporter;
        this.xmlExporter = xmlExporter;
    }

    public void export(String format, String jobTs, String search, String table,
                       DiffRepository.Filters filters, HttpServletResponse response) throws Exception {
        String stamp = LocalDateTime.now().format(STAMP);

        if ("xml".equalsIgnoreCase(format)) {
            response.setContentType("application/xml; charset=UTF-8");
            response.setHeader("Content-Disposition",
                    "attachment; filename=\"difference_log_" + stamp + ".xml\"");
            xmlExporter.write(response.getOutputStream(), jobTs, search, table, filters);
        } else {
            response.setContentType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
            response.setHeader("Content-Disposition",
                    "attachment; filename=\"difference_log_" + stamp + ".xlsx\"");
            excelExporter.write(response.getOutputStream(), jobTs, search, table, filters);
        }
    }
}

