package linkmesh.ingest;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Writes page records into one partition directory.
 *
 * Many pages per file, since a URL line starts a new record. One file per page
 * would mean hundreds of thousands of tiny files, which is slow to create and
 * slow to walk, especially on Windows.
 *
 * Nested bucket tree rather than a flat directory, so the ForkJoin scan in the
 * map stage has something to split on. The nesting is uneven on purpose.
 */
public final class PartitionWriter implements Closeable {
    private static final int BUCKETS = 7;
    private static final int SUBS = 3;

    private final Path root;
    private final int pagesPerFile;

    private BufferedWriter writer;
    private int fileIndex;
    private int pagesInCurrentFile;
    private long pagesWritten;
    private long linksWritten;

    public PartitionWriter(Path root, int pagesPerFile) throws IOException {
        this.root = root;
        this.pagesPerFile = Math.max(1, pagesPerFile);
        Files.createDirectories(root);
    }

    public void write(String sourceUrl, List<String> targets) throws IOException {
        if (targets.isEmpty()) return;
        if (writer == null || pagesInCurrentFile >= pagesPerFile) rollFile();

        writer.write("URL ");
        writer.write(sourceUrl);
        writer.newLine();
        for (String target : targets) {
            writer.write("LINK ");
            writer.write(target);
            writer.newLine();
        }
        pagesInCurrentFile++;
        pagesWritten++;
        linksWritten += targets.size();
    }

    private void rollFile() throws IOException {
        if (writer != null) writer.close();
        Path directory = root.resolve("bucket-" + (fileIndex % BUCKETS))
                             .resolve("sub-" + (fileIndex % SUBS));
        Files.createDirectories(directory);
        Path file = directory.resolve(String.format("pages-%06d.page", fileIndex));
        writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8);
        fileIndex++;
        pagesInCurrentFile = 0;
    }

    public long pagesWritten() { return pagesWritten; }

    public long linksWritten() { return linksWritten; }

    public boolean isEmpty() { return pagesWritten == 0; }

    public Path root() { return root; }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }
}
