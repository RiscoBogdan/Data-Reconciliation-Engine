package com.reconciler.service.export;

import com.reconciler.repository.DiffRepository;
import org.springframework.stereotype.Component;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

/**
 * Streams the filtered Difference Log to XML row-by-row so exports of millions
 * of rows never buffer everything in memory.
 */
@Component
public class XmlDiffExporter {

    private final DiffRepository diffRepo;

    public XmlDiffExporter(DiffRepository diffRepo) {
        this.diffRepo = diffRepo;
    }

    public void write(OutputStream out, String jobTs, String search, String table,
                      DiffRepository.Filters filters) throws Exception {
        try (Writer w = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
            w.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<differences>\n");
            diffRepo.streamFiltered(jobTs, search, table, filters, r -> {
                try {
                    w.write("  <diff>\n");
                    for (String col : ExportColumns.HEADERS) {
                        w.write("    <" + col + ">"
                                + xmlEscape(String.valueOf(r.getOrDefault(col, "")))
                                + "</" + col + ">\n");
                    }
                    w.write("  </diff>\n");
                } catch (Exception e) {
                    throw new RuntimeException("XML export write failed", e);
                }
            });
            w.write("</differences>\n");
        }
    }

    private static String xmlEscape(String v) {
        if (v == null) return "";
        return v.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;").replace("'", "&apos;");
    }
}

