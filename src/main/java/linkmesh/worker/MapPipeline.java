package linkmesh.worker;

import linkmesh.common.Log;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Map stage for one partition on one node.
 *
 *   partition directory
 *        |  ForkJoinPool, recursive work-stealing scan
 *        v
 *   BoundedTaskQueue of file paths      <- backpressure
 *        |  fixed platform thread pool
 *        v
 *   parse page, emit edges -> ShuffleWriter
 *
 * ForkJoin for discovery because the tree is uneven and stealing balances it.
 * A fixed pool for parsing because those tasks are uniform and stealing would
 * only add overhead. Platform threads, not virtual, since parsing is CPU-bound.
 */
public final class MapPipeline {
    private static final Log log = Log.of("map");

    public record Result(long files, long pages, long links, long producerWaits, long consumerWaits,
                         int queueHighWater, long batches, long records, long elapsedMillis) {}

    private final Path partitionRoot;
    private final ShuffleWriter shuffle;
    private final int parserThreads;
    private final int queueCapacity;
    private final int parseDelayMillis;

    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicLong files = new AtomicLong();
    private final AtomicLong pages = new AtomicLong();
    private final AtomicLong links = new AtomicLong();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    private volatile BoundedTaskQueue<Path> queue;
    private volatile ForkJoinPool scanPool;
    private volatile ExecutorService parserPool;

    public MapPipeline(Path partitionRoot, ShuffleWriter shuffle,
                       int parserThreads, int queueCapacity, int parseDelayMillis) {
        this.partitionRoot = partitionRoot;
        this.shuffle = shuffle;
        this.parserThreads = Math.max(1, parserThreads);
        this.queueCapacity = Math.max(1, queueCapacity);
        this.parseDelayMillis = Math.max(0, parseDelayMillis);
    }

    public Result run() throws Exception {
        long start = System.nanoTime();
        queue = new BoundedTaskQueue<>(queueCapacity);
        AtomicLong parserNumber = new AtomicLong();
        parserPool = Executors.newFixedThreadPool(parserThreads, runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("parser-" + parserNumber.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        scanPool = new ForkJoinPool(Math.max(2, Runtime.getRuntime().availableProcessors() / 2));

        try {
            List<Future<?>> parsers = new ArrayList<>();
            for (int i = 0; i < parserThreads; i++) parsers.add(parserPool.submit(this::parserLoop));

            try {
                scanPool.invoke(new ScanTask(partitionRoot));
            } finally {
                // Consumers must be released even if discovery blew up, or the
                // parser futures below would block forever waiting on take().
                queue.close();
            }

            for (Future<?> parser : parsers) parser.get();

            Throwable thrown = failure.get();
            if (thrown != null) throw asException(thrown);
            if (cancelled.get()) throw new CancellationException("map stage cancelled");

            shuffle.flushAll();

            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            return new Result(files.get(), pages.get(), links.get(), queue.producerWaits(), queue.consumerWaits(),
                    queue.highWaterMark(), shuffle.batchesSent(), shuffle.recordsSent(), elapsed);
        } finally {
            shutdownPools();
        }
    }

    private void parserLoop() {
        try {
            while (!cancelled.get()) {
                Path file = queue.take();
                if (file == null) return;
                if (parseDelayMillis > 0) Thread.sleep(parseDelayMillis);
                PageParser.Parsed parsed = PageParser.parse(file, record -> {
                    try {
                        shuffle.emit(record);
                    } catch (IOException e) {
                        throw new UncheckedShuffleException(e);
                    }
                });
                files.incrementAndGet();
                pages.addAndGet(parsed.pages());
                links.addAndGet(parsed.links());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (UncheckedShuffleException e) {
            recordFailure(e.getCause());
        } catch (Throwable t) {
            recordFailure(t);
        }
    }

    /** First failure wins and cancels the rest, so the reported cause is the real one. */
    private void recordFailure(Throwable t) {
        if (failure.compareAndSet(null, t)) {
            log.warn("map failure: %s: %s", t.getClass().getSimpleName(), t.getMessage());
            cancel();
        }
    }

    public void cancel() {
        if (!cancelled.compareAndSet(false, true)) return;
        BoundedTaskQueue<Path> q = queue;
        if (q != null) q.close();
        ForkJoinPool scan = scanPool;
        if (scan != null) scan.shutdownNow();
    }

    public boolean isCancelled() { return cancelled.get(); }

    private void shutdownPools() {
        ForkJoinPool scan = scanPool;
        if (scan != null) scan.shutdownNow();
        ExecutorService parsers = parserPool;
        if (parsers != null) parsers.shutdownNow();
    }

    private static Exception asException(Throwable t) {
        return t instanceof Exception e ? e : new ExecutionException(t);
    }

    /**
     * Recursive directory discovery. Splits at every subdirectory so the pool can
     * steal deep subtrees, and checks the cancel flag at each entry so a
     * CANCEL_TASK stops the scan promptly rather than after the whole walk.
     */
    private final class ScanTask extends RecursiveAction {
        private final Path directory;

        ScanTask(Path directory) { this.directory = directory; }

        @Override
        protected void compute() {
            if (cancelled.get()) return;
            List<ScanTask> subdirectories = new ArrayList<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
                for (Path child : stream) {
                    if (cancelled.get()) return;
                    if (Files.isDirectory(child)) {
                        subdirectories.add(new ScanTask(child));
                    } else if (child.getFileName().toString().endsWith(".page")) {
                        if (!queue.put(child)) return;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (IOException e) {
                recordFailure(e);
                return;
            }
            if (!subdirectories.isEmpty()) invokeAll(subdirectories);
        }
    }

    /** Lets an IOException escape the Consumer that PageParser hands to the sink. */
    private static final class UncheckedShuffleException extends RuntimeException {
        UncheckedShuffleException(IOException cause) { super(cause); }
    }
}
