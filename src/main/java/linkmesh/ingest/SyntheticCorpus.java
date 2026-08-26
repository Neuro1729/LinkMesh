package linkmesh.ingest;

import linkmesh.common.Hashing;
import linkmesh.common.Log;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates a synthetic corpus in the same layout the real ingesters produce,
 * for benchmarking without downloading anything.
 *
 * Targets follow a power law, not a uniform draw. Uniform targets hide the one
 * property of real web data that matters most here: a few URLs collect most of
 * the backlinks, which is what makes one reducer finish long after the others.
 */
public final class SyntheticCorpus {
    private static final Log log = Log.of("gen");

    public record Stats(long pages, long links, int partitions) {}

    public static Stats generate(Path outputDir, int pages, int partitions, int linksPerPage,
                                 int pagesPerFile, long seed, double skew) throws IOException {
        Random random = new Random(seed);
        List<PartitionWriter> writers = new ArrayList<>(partitions);
        for (int i = 0; i < partitions; i++) {
            writers.add(new PartitionWriter(outputDir.resolve(String.format("part-%03d", i)), pagesPerFile));
        }

        long totalLinks = 0;
        try {
            for (int i = 0; i < pages; i++) {
                String source = "https://site" + (i % 500) + ".example/page/" + i;
                List<String> targets = new ArrayList<>(linksPerPage);
                for (int j = 0; j < linksPerPage; j++) {
                    int target = skewedTarget(random, pages, skew);
                    if (target == i) continue;
                    targets.add("https://site" + (target % 500) + ".example/page/" + target);
                }
                if (targets.isEmpty()) continue;
                writers.get(Hashing.bucket(source, partitions)).write(source, targets);
                totalLinks += targets.size();
            }
        } finally {
            for (PartitionWriter writer : writers) writer.close();
        }

        log.info("generated %,d pages and %,d links across %d partitions at %s",
                pages, totalLinks, partitions, outputDir);
        return new Stats(pages, totalLinks, partitions);
    }

    /**
     * Draws a target with a Zipf-like bias toward low indices, so a small set of
     * pages accumulates most of the inbound links.
     */
    private static int skewedTarget(Random random, int pages, double skew) {
        if (skew <= 0) return random.nextInt(pages);
        double u = random.nextDouble();
        int index = (int) (pages * Math.pow(u, 1.0 + skew));
        return Math.min(pages - 1, Math.max(0, index));
    }
}
