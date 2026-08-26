package linkmesh.ingest;

import linkmesh.common.Hashing;
import linkmesh.common.Log;
import linkmesh.common.Text;
import linkmesh.proto.*;
import linkmesh.storage.LocalStore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Turns a corpus into partitions and pushes them into the cluster.
 *
 * Sharded by hash of the source URL, so each page lands in one partition and the
 * partitions come out roughly even. Note the shuffle hashes the target URL
 * instead: regrouping an edge from its source partition to its target reducer is
 * the whole job of the map stage.
 *
 * Placement comes from the controller one partition at a time, so that decision
 * lives in one place and the ingester pushes straight to the chosen nodes rather
 * than funnelling every byte through the controller.
 */
public final class Ingestor {
    private static final Log log = Log.of("ingest");

    public enum Format { WARC, WIKIPEDIA }

    public static final class Config {
        public Endpoint controller;
        public int partitions = 32;
        public int pagesPerFile = 200;
        public boolean externalOnly = true;
        public long maxPages = Long.MAX_VALUE;
        public Path staging = Path.of("build/ingest-staging");
        public String prefix = "part";
    }

    public record Stats(long recordsSeen, long pagesWritten, long linksWritten,
                        long partitionsPushed, long bytesPushed, long elapsedMillis) {}

    private static final class Counters {
        long recordsSeen;
        long pagesWritten;
        long linksWritten;
        long skippedNoLinks;
    }

    private final Config config;
    private final ConnectionPool pool = new ConnectionPool(5000, 120_000);

    public Ingestor(Config config) {
        this.config = config;
    }

    public Stats ingest(List<Path> inputs) throws IOException {
        return shard(inputs, Format.WARC, true);
    }

    public Stats ingestWikipedia(List<Path> inputs) throws IOException {
        return shard(inputs, Format.WIKIPEDIA, true);
    }

    /**
     * Parses a corpus into partition directories on local disk without pushing
     * it anywhere. Useful when the same corpus is loaded repeatedly, since
     * re-parsing gigabytes of source HTML for every run would dominate the
     * measurement of everything else.
     */
    public Stats extractOnly(List<Path> inputs, Format format) throws IOException {
        return shard(inputs, format, false);
    }

    private Stats shard(List<Path> inputs, Format format, boolean push) throws IOException {
        long start = System.nanoTime();
        LocalStore.deleteRecursively(config.staging);
        Files.createDirectories(config.staging);
        if (!push) log.info("extracting to %s (no cluster push)", config.staging.toAbsolutePath());

        List<PartitionWriter> writers = new ArrayList<>(config.partitions);
        for (int i = 0; i < config.partitions; i++) {
            writers.add(new PartitionWriter(config.staging.resolve(partitionId(i)), config.pagesPerFile));
        }

        Counters counters = new Counters();
        try {
            for (Path input : inputs) {
                log.info("reading %s", input.getFileName());
                boolean more = format == Format.WARC
                        ? readWarc(input, writers, counters)
                        : readWikipedia(input, writers, counters);
                if (!more) break;
            }
        } finally {
            for (PartitionWriter writer : writers) writer.close();
        }

        log.info("extracted %,d pages and %,d links from %,d records (%,d had no usable links)",
                counters.pagesWritten, counters.linksWritten, counters.recordsSeen, counters.skippedNoLinks);
        if (counters.pagesWritten == 0) {
            throw new IOException("no pages extracted; check the input format matches the flag you used");
        }

        long partitionsPushed = 0;
        long bytesPushed = 0;
        for (int i = 0; i < config.partitions; i++) {
            PartitionWriter writer = writers.get(i);
            if (writer.isEmpty()) continue;
            if (push) {
                bytesPushed += push(partitionId(i), writer.root());
            } else {
                bytesPushed += sizeOf(writer.root());
            }
            partitionsPushed++;
        }

        if (push) LocalStore.deleteRecursively(config.staging);
        long elapsed = (System.nanoTime() - start) / 1_000_000L;
        return new Stats(counters.recordsSeen, counters.pagesWritten, counters.linksWritten,
                partitionsPushed, bytesPushed, elapsed);
    }

    /** Returns false once the page limit is reached. */
    private boolean readWarc(Path input, List<PartitionWriter> writers, Counters counters) throws IOException {
        try (WarcReader reader = WarcReader.open(input)) {
            WarcRecord record;
            while ((record = reader.next()) != null) {
                counters.recordsSeen++;
                if (!record.isResponse() || !record.isHtml()) continue;
                String url = record.targetUri();
                if (url == null || url.isBlank()) continue;
                if (!accept(url, record.text(), config.externalOnly, false, writers, counters)) return false;
            }
        }
        return true;
    }

    private boolean readWikipedia(Path input, List<PartitionWriter> writers, Counters counters) throws IOException {
        try (WikipediaDumpReader reader = WikipediaDumpReader.open(input)) {
            WikipediaDumpReader.Article article;
            while ((article = reader.next()) != null) {
                counters.recordsSeen++;
                // Wikipedia articles link to other Wikipedia articles, so the
                // usual same-site filter would discard the entire graph. Here
                // the internal links are the point.
                if (!accept(article.url(), article.html(), false, true, writers, counters)) return false;
            }
        }
        return true;
    }

    private boolean accept(String url, String html, boolean externalOnly, boolean articlesOnly,
                           List<PartitionWriter> writers, Counters counters) throws IOException {
        HtmlLinks.Page page = HtmlLinks.extract(url, html, externalOnly);
        if (page == null) { counters.skippedNoLinks++; return true; }

        List<String> targets = page.targets();
        if (articlesOnly) {
            List<String> filtered = new ArrayList<>(targets.size());
            for (String target : targets) {
                if (WikipediaLinks.isArticle(target)) filtered.add(target);
            }
            targets = filtered;
        }
        if (targets.isEmpty()) { counters.skippedNoLinks++; return true; }

        int partition = Hashing.bucket(page.sourceUrl(), config.partitions);
        writers.get(partition).write(page.sourceUrl(), targets);
        counters.pagesWritten++;
        counters.linksWritten += targets.size();

        if (counters.pagesWritten % 20_000 == 0) {
            log.info("%,d pages, %,d links extracted", counters.pagesWritten, counters.linksWritten);
        }
        if (counters.pagesWritten >= config.maxPages) {
            log.info("reached --maxPages limit of %,d", config.maxPages);
            return false;
        }
        return true;
    }

    /** Ingests a directory of already-formatted .page files, one partition per subdirectory. */
    public Stats ingestPrepared(Path directory) throws IOException {
        long start = System.nanoTime();
        long partitionsPushed = 0;
        long bytesPushed = 0;
        try (var stream = Files.list(directory)) {
            for (Path child : stream.filter(Files::isDirectory).sorted().toList()) {
                bytesPushed += push(child.getFileName().toString(), child);
                partitionsPushed++;
            }
        }
        return new Stats(0, 0, 0, partitionsPushed, bytesPushed, (System.nanoTime() - start) / 1_000_000L);
    }

    private static long sizeOf(Path directory) throws IOException {
        try (var stream = Files.walk(directory)) {
            long total = 0;
            for (Path file : stream.filter(Files::isRegularFile).toList()) total += Files.size(file);
            return total;
        }
    }

    private String partitionId(int index) {
        return String.format("%s-%03d", config.prefix, index);
    }

    /** Asks the controller where this partition belongs, then pushes to each holder. */
    private long push(String partitionId, Path directory) throws IOException {
        Message placement = pool.request(config.controller, Message.of(Verbs.PLACE, "partition", partitionId));
        if (placement.isError()) throw new IOException("placement failed: " + placement.get("reason", "?"));
        List<Endpoint> targets = new ArrayList<>();
        List<String> nodeIds = new ArrayList<>();
        for (String item : placement.bodyText().split(";")) {
            if (item.isBlank()) continue;
            String[] parts = item.split(",", -1);
            nodeIds.add(parts[0]);
            targets.add(new Endpoint(parts[1], Integer.parseInt(parts[2])));
        }
        if (targets.isEmpty()) {
            throw new IOException("controller returned no placement targets; are any nodes running?");
        }

        ByteArrayOutputStream buffer = new ByteArrayOutputStream(1 << 20);
        Archive.Written written = Archive.pack(directory, buffer);
        byte[] archive = buffer.toByteArray();

        for (int i = 0; i < targets.size(); i++) {
            Message reply = pool.request(targets.get(i),
                    Message.of(Verbs.STORE_PUT, "partition", partitionId, "sha", written.sha256())
                            .withBody(archive));
            if (reply.isError()) {
                throw new IOException("store of " + partitionId + " on " + nodeIds.get(i)
                        + " failed: " + reply.get("reason", "?"));
            }
        }
        log.info("pushed %s (%s, %d files) to %s", partitionId,
                Text.humanBytes(written.bytes()), written.files(), nodeIds);
        return written.bytes() * targets.size();
    }

    public void close() {
        pool.close();
    }
}
