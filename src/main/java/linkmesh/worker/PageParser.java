package linkmesh.worker;

import linkmesh.common.MapRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

/**
 * Reads one .page record and emits an edge per outbound link.
 *
 * The format is deliberately trivial so that the distributed machinery, not the
 * parsing, is what the benchmark measures:
 *
 *   URL https://example.com/a
 *   LINK https://example.com/b
 *   LINK https://other.example/c
 *
 * Real HTML is converted into this shape once, at ingest time, where URL
 * resolution and normalization can be done with the page base URL in hand.
 */
public final class PageParser {
    private PageParser() {}

    /**
     * A file holds many page records, so pages and files are counted separately.
     * Reporting file count as page count would understate throughput by whatever
     * the ingester chose for pagesPerFile.
     */
    public record Parsed(int pages, int links) {}

    public static Parsed parse(Path file, Consumer<MapRecord> sink) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String source = null;
            int pages = 0;
            int links = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("URL ")) {
                    source = line.substring(4).trim();
                    if (!source.isEmpty()) pages++;
                } else if (line.startsWith("LINK ") && source != null) {
                    String target = line.substring(5).trim();
                    if (!target.isEmpty()) {
                        sink.accept(new MapRecord(target, source));
                        links++;
                    }
                }
            }
            return new Parsed(pages, links);
        }
    }
}
