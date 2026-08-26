package linkmesh.ingest;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Reads a plain edge list, the format most public graph benchmarks ship in.
 *
 *   # comment lines are ignored
 *   0    11342
 *   0    824020
 *   11342    0
 *
 * Whitespace or tab separated, optionally gzipped. This is what lets standard
 * datasets (SNAP, and anything exported as an edge list) run through the same
 * pipeline as HTML input, so the distributed machinery can be measured on graphs
 * with known published shapes.
 *
 * Consecutive rows sharing a source are grouped into one page record. Most
 * published files are already sorted by source so that groups them fully; an
 * unsorted file just yields several records per source, which is still correct
 * because the reducer merges into a Set either way.
 */
public final class EdgeListReader implements Closeable {

    /** One source node and the targets it points at. */
    public record Adjacency(String source, List<String> targets) {}

    private final BufferedReader reader;
    private long linesRead;
    private long malformed;

    private String pendingSource;
    private String pendingTarget;

    public EdgeListReader(Path file) throws IOException {
        var stream = Files.newInputStream(file);
        String name = file.getFileName().toString().toLowerCase();
        var decoded = name.endsWith(".gz")
                ? new GZIPInputStream(stream, 1 << 16)
                : stream;
        this.reader = new BufferedReader(new InputStreamReader(decoded, StandardCharsets.UTF_8), 1 << 20);
    }

    public long linesRead() { return linesRead; }

    public long malformed() { return malformed; }

    /** Returns the next source and its targets, or null at end of file. */
    public Adjacency next() throws IOException {
        String source = pendingSource;
        List<String> targets = new ArrayList<>();
        if (source != null) {
            targets.add(pendingTarget);
            pendingSource = null;
            pendingTarget = null;
        }

        String line;
        while ((line = reader.readLine()) != null) {
            if (line.isEmpty() || line.charAt(0) == '#' || line.charAt(0) == '%') continue;
            linesRead++;

            int split = indexOfSeparator(line);
            if (split < 0) { malformed++; continue; }
            String from = line.substring(0, split);
            String to = line.substring(split + 1).trim();
            int extra = indexOfSeparator(to);
            if (extra >= 0) to = to.substring(0, extra);
            if (from.isEmpty() || to.isEmpty()) { malformed++; continue; }

            if (source == null) {
                source = from;
                targets.add(to);
            } else if (from.equals(source)) {
                targets.add(to);
            } else {
                // Start of the next group. Hold it for the following call.
                pendingSource = from;
                pendingTarget = to;
                return new Adjacency(source, targets);
            }
        }
        return source == null ? null : new Adjacency(source, targets);
    }

    private static int indexOfSeparator(String line) {
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '\t' || c == ' ' || c == ',') return i;
        }
        return -1;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
