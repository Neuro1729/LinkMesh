package linkmesh.worker;

import linkmesh.cluster.NodeRole;
import linkmesh.common.Log;
import linkmesh.common.Text;
import linkmesh.proto.*;
import linkmesh.storage.LocalStore;
import linkmesh.storage.PartitionMeta;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A cluster node: storage (holds replicas, serves transfers), compute (runs map
 * tasks), and optionally reducer for a job.
 *
 * Keeping all three in one process is what allows locality scheduling, since the
 * controller can place a task where the bytes already are.
 *
 * Needs one flag to start, the controller address. Everything else is derived,
 * and the node reports its existing replicas on join so a restart is cheap.
 */
public final class Worker implements AutoCloseable {
    private static final Log log = Log.of("worker");

    public static final class Config {
        public String nodeId;
        public int port;
        public String advertiseHost;
        public Endpoint controller;
        public Path dataDir;
        /**
         * Concurrent map tasks per node.
         *
         * This matters more than parserThreads does. A task builds its own thread
         * pools and tears them down again, and a single partition often does not
         * have enough files to keep a wide parser pool busy, so throughput is
         * governed by how many partitions are in flight rather than how many
         * threads each one gets. Measured on an 8-core box, going from 1 slot to
         * 4 cut the map stage from 12.8s to 3.9s, while widening the parser pool
         * from 2 threads to 8 changed almost nothing.
         */
        public int slots = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
        public NodeRole role = NodeRole.AUTO;
        public int parserThreads = 2;
        public int queueCapacity = 512;
        /**
         * Records per shuffle batch.
         *
         * Each batch is one blocking request/response, so this sets how many
         * network round trips the shuffle pays. On loopback that barely matters
         * and 512 was fine. Across real machines it dominates: on a two-machine
         * cluster over a VPN, raising this from 512 to 8192 cut the map stage
         * from 40.0s to 3.9s, because it removed about 7,000 round trips.
         */
        public int shuffleBatch = 4096;
        public int parseDelayMillis = 0;
        public long heartbeatMillis = 1000;
        public long gossipMillis = 1000;
        public long suspectMillis = 3000;
        public long deadMillis = 8000;
        public int gossipFanout = 2;
    }

    private final Config config;
    private final LocalStore store;
    private final ConnectionPool pool = new ConnectionPool(3000, 120_000);
    private final MessageServer server;

    private final Map<String, ReducerStore> reducerStores = new ConcurrentHashMap<>();
    private final Map<String, MapPipeline> runningTasks = new ConcurrentHashMap<>();
    private final ExecutorService taskExecutor;
    private final ExecutorService transferExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService heartbeatScheduler =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "heartbeat");
                thread.setDaemon(true);
                return thread;
            });

    private final AtomicLong heartbeatCounter = new AtomicLong();
    private volatile Gossip gossip;
    private volatile Connection controlConnection;
    private volatile Endpoint self;
    private volatile boolean closed;

    private Worker(Config config, LocalStore store, MessageServer server, Endpoint self,
                   ExecutorService taskExecutor) {
        this.config = config;
        this.store = store;
        this.server = server;
        this.self = self;
        this.taskExecutor = taskExecutor;
    }

    public static Worker start(Config config) throws IOException {
        LocalStore store = new LocalStore(config.dataDir);
        ExecutorService taskExecutor = Executors.newFixedThreadPool(Math.max(1, config.slots), runnable -> {
            Thread thread = new Thread(runnable, "task-runner");
            thread.setDaemon(true);
            return thread;
        });

        Worker[] holder = new Worker[1];
        MessageServer server = new MessageServer("worker", config.port,
                new MessageServer.Handler() {
                    @Override
                    public Message handle(Message request, Connection connection) throws Exception {
                        return holder[0].dispatch(request, connection);
                    }
                });
        int boundPort = server.start();

        String host = config.advertiseHost != null ? config.advertiseHost : Nets.detectAdvertiseAddress();
        Endpoint self = new Endpoint(host, boundPort);
        if (config.nodeId == null || config.nodeId.isBlank()) {
            config.nodeId = Nets.defaultNodeId(boundPort);
        }

        Worker worker = new Worker(config, store, server, self, taskExecutor);
        holder[0] = worker;
        worker.gossip = new Gossip(config.nodeId, self, config.controller, worker.pool,
                config.suspectMillis, config.deadMillis, config.gossipFanout);

        log.info("node %s listening on %s (role=%s, data=%s, %d partitions held, %s)",
                config.nodeId, self, config.role, store.root(), store.size(), Text.humanBytes(store.totalBytes()));

        worker.join();
        worker.gossip.start(config.gossipMillis);
        worker.heartbeatScheduler.scheduleAtFixedRate(worker::heartbeat,
                config.heartbeatMillis, config.heartbeatMillis, TimeUnit.MILLISECONDS);
        return worker;
    }

    public Endpoint endpoint() { return self; }

    public String nodeId() { return config.nodeId; }

    public LocalStore store() { return store; }

    /**
     * Opens the long-lived control connection and announces this node.
     *
     * This connection is deliberately never returned to the pool. It is the
     * liveness channel: if this process dies, the controller sees the socket
     * close and evicts the node immediately, without waiting for a timeout.
     */
    private void join() {
        for (int attempt = 1; !closed; attempt++) {
            try {
                Connection connection = Connection.connect(config.controller, 3000, 0);
                Message hello = Message.of(Verbs.HELLO,
                                "node", config.nodeId,
                                "host", self.host(),
                                "port", Integer.toString(self.port()),
                                "slots", Integer.toString(config.slots),
                                "role", config.role.name(),
                                "bytes", Long.toString(store.totalBytes()))
                        .withBody(String.join(",", store.ids()));
                Message reply = connection.request(hello);
                if (reply.isError()) throw new ProtocolException(reply.get("reason", "rejected"));
                controlConnection = connection;
                if (gossip != null) gossip.seed(reply.bodyText());
                log.info("joined cluster via controller %s as %s", config.controller, config.nodeId);
                return;
            } catch (IOException | ProtocolException e) {
                if (attempt == 1 || attempt % 10 == 0) {
                    log.warn("controller %s unreachable (%s), retrying", config.controller, e.getMessage());
                }
                try {
                    Thread.sleep(Math.min(5000, 250L * attempt));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void heartbeat() {
        if (closed) return;
        Connection connection = controlConnection;
        if (connection == null || !connection.isUsable()) {
            log.warn("control connection lost, rejoining");
            controlConnection = null;
            join();
            return;
        }
        try {
            Message reply = connection.request(Message.of(Verbs.HEARTBEAT,
                    "node", config.nodeId,
                    "counter", Long.toString(heartbeatCounter.incrementAndGet()),
                    "running", Integer.toString(runningTasks.size()),
                    "partitions", Integer.toString(store.size()),
                    "bytes", Long.toString(store.totalBytes())));
            if (reply.hasBody() && gossip != null) gossip.seed(reply.bodyText());
        } catch (IOException e) {
            log.warn("heartbeat failed: %s", e.getMessage());
            connection.close();
            controlConnection = null;
        }
    }

    private Message dispatch(Message request, Connection connection) throws Exception {
        return switch (request.verb()) {
            case Verbs.GOSSIP -> {
                if (gossip != null) gossip.merge(request.get("from", "?"), request.bodyText());
                yield Message.ok();
            }
            case Verbs.PEERS -> {
                if (gossip != null) gossip.seed(request.bodyText());
                yield Message.ok();
            }
            case Verbs.STORE_LIST -> Message.ok("count", Integer.toString(store.size()))
                    .withBody(String.join(",", store.ids()));
            case Verbs.STORE_PUT -> {
                PartitionMeta meta = store.store(request.require("partition"),
                        new java.io.ByteArrayInputStream(request.body()), request.get("sha"));
                yield Message.ok("bytes", Long.toString(meta.bytes()), "files", Long.toString(meta.files()));
            }
            case Verbs.STORE_FETCH -> {
                String partition = request.require("partition");
                if (!store.has(partition)) yield Message.error("not held: " + partition);
                ByteArrayOutputStream buffer = new ByteArrayOutputStream(1 << 20);
                Archive.Written written = store.pack(partition, buffer);
                yield Message.ok("sha", written.sha256(), "files", Long.toString(written.files()))
                        .withBody(buffer.toByteArray());
            }
            case Verbs.STORE_DROP -> Message.ok("dropped", Boolean.toString(store.drop(request.require("partition"))));
            case Verbs.REPLICATE -> {
                acceptReplication(request);
                yield Message.ok();
            }
            case Verbs.RUN_TASK -> {
                acceptTask(request);
                yield Message.ok();
            }
            case Verbs.CANCEL_TASK -> {
                MapPipeline pipeline = runningTasks.get(request.require("attempt"));
                if (pipeline != null) {
                    log.info("cancelling attempt %s", request.require("attempt"));
                    pipeline.cancel();
                }
                yield Message.ok();
            }
            case Verbs.MAP_BATCH -> {
                ReducerStore reducer = reducerStores.computeIfAbsent(request.require("job"), k -> new ReducerStore());
                int applied = reducer.accept(request.body());
                yield Message.ok("applied", Integer.toString(applied));
            }
            case Verbs.FINALIZE -> {
                finalizeReducer(request);
                yield Message.ok();
            }
            case Verbs.STATUS -> Message.ok(
                    "node", config.nodeId,
                    "partitions", Integer.toString(store.size()),
                    "bytes", Long.toString(store.totalBytes()),
                    "running", Integer.toString(runningTasks.size()));
            default -> Message.error("unknown verb " + request.verb());
        };
    }

    /** Push one of our replicas to a peer, in the background so the controller is not blocked. */
    private void acceptReplication(Message request) {
        String partition = request.require("partition");
        Endpoint target = new Endpoint(request.require("targetHost"), Integer.parseInt(request.require("targetPort")));
        transferExecutor.submit(() -> {
            try {
                if (!store.has(partition)) {
                    log.warn("asked to replicate %s which we do not hold", partition);
                    return;
                }
                ByteArrayOutputStream buffer = new ByteArrayOutputStream(1 << 20);
                Archive.Written written = store.pack(partition, buffer);
                Message put = Message.of(Verbs.STORE_PUT, "partition", partition, "sha", written.sha256())
                        .withBody(buffer.toByteArray());
                pool.request(target, put).orThrow();
                log.info("replicated %s to %s (%s)", partition, target, Text.humanBytes(written.bytes()));
            } catch (IOException | RuntimeException e) {
                log.warn("replication of %s to %s failed: %s", partition, target, e.getMessage());
            }
        });
    }

    private void acceptTask(Message request) {
        String jobId = request.require("job");
        String attemptId = request.require("attempt");
        String partition = request.require("partition");
        List<Endpoint> reducers = decodeReducers(request.require("reducers"));
        String sourceHost = request.get("sourceHost");
        int sourcePort = request.getInt("sourcePort", 0);
        int parserThreads = request.getInt("parserThreads", config.parserThreads);
        int queueCapacity = request.getInt("queueCapacity", config.queueCapacity);
        int shuffleBatch = request.getInt("shuffleBatch", config.shuffleBatch);
        int parseDelay = request.getInt("parseDelay", config.parseDelayMillis);

        taskExecutor.submit(() -> runTask(jobId, attemptId, partition, reducers,
                sourceHost, sourcePort, parserThreads, queueCapacity, shuffleBatch, parseDelay));
    }

    private void runTask(String jobId, String attemptId, String partition, List<Endpoint> reducers,
                         String sourceHost, int sourcePort, int parserThreads, int queueCapacity,
                         int shuffleBatch, int parseDelay) {
        try {
            if (!store.has(partition)) {
                if (sourceHost == null || sourcePort <= 0) {
                    reportFailure(jobId, attemptId, partition, "partition not held and no source given");
                    return;
                }
                fetchPartition(partition, new Endpoint(sourceHost, sourcePort));
            }

            // Pin the replica for the whole scan. Re-replication and the placement
            // rebalancer both target partitions this node holds, and a task reads
            // by path, so an unpinned partition can be dropped or republished
            // mid-scan and the files disappear underneath the parser.
            if (!store.acquire(partition)) {
                reportFailure(jobId, attemptId, partition, "partition vanished before the task could start");
                return;
            }
            try {
                Path root = store.pathOf(partition);
                try (ShuffleWriter shuffle = new ShuffleWriter(jobId, reducers, pool, shuffleBatch)) {
                    MapPipeline pipeline = new MapPipeline(root, shuffle, parserThreads, queueCapacity, parseDelay);
                    runningTasks.put(attemptId, pipeline);
                    try {
                        MapPipeline.Result result = pipeline.run();
                        reportSuccess(jobId, attemptId, partition, result);
                    } finally {
                        runningTasks.remove(attemptId);
                    }
                }
            } finally {
                store.release(partition);
            }
        } catch (CancellationException e) {
            log.info("attempt %s cancelled", attemptId);
            reportFailure(jobId, attemptId, partition, "cancelled");
        } catch (Exception e) {
            log.warn("attempt %s failed: %s: %s", attemptId, e.getClass().getSimpleName(), e.getMessage());
            reportFailure(jobId, attemptId, partition, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    /** On-demand pull so an idle node can take work for a partition it does not hold yet. */
    private void fetchPartition(String partition, Endpoint source) throws IOException {
        log.info("fetching %s from %s", partition, source);
        long start = System.nanoTime();
        Message reply = pool.request(source, Message.of(Verbs.STORE_FETCH, "partition", partition));
        if (reply.isError()) throw new IOException("fetch of " + partition + " failed: " + reply.get("reason", "?"));
        PartitionMeta meta = store.store(partition, new java.io.ByteArrayInputStream(reply.body()), reply.get("sha"));
        log.info("fetched %s (%s in %d ms)", partition, Text.humanBytes(meta.bytes()),
                (System.nanoTime() - start) / 1_000_000L);
    }

    private void reportSuccess(String jobId, String attemptId, String partition, MapPipeline.Result result) {
        try {
            pool.request(config.controller, Message.of(Verbs.TASK_DONE,
                    "node", config.nodeId,
                    "job", jobId,
                    "attempt", attemptId,
                    "partition", partition,
                    "elapsed", Long.toString(result.elapsedMillis()),
                    "files", Long.toString(result.files()),
                    "pages", Long.toString(result.pages()),
                    "links", Long.toString(result.links()),
                    "producerWaits", Long.toString(result.producerWaits()),
                    "queueHighWater", Integer.toString(result.queueHighWater()),
                    "batches", Long.toString(result.batches())));
            log.info("%s done: %d pages in %d files, %d links, %d ms (queue high water %d, producer waits %d)",
                    partition, result.pages(), result.files(), result.links(), result.elapsedMillis(),
                    result.queueHighWater(), result.producerWaits());
        } catch (IOException e) {
            log.error("could not report completion of %s: %s", attemptId, e.getMessage());
        }
    }

    private void reportFailure(String jobId, String attemptId, String partition, String reason) {
        try {
            pool.request(config.controller, Message.of(Verbs.TASK_FAILED,
                    "node", config.nodeId, "job", jobId, "attempt", attemptId,
                    "partition", partition, "reason", reason));
        } catch (IOException e) {
            log.error("could not report failure of %s: %s", attemptId, e.getMessage());
        }
    }

    /** Streams this reducer partition of the index back to the controller. */
    private void finalizeReducer(Message request) {
        String jobId = request.require("job");
        int reducerId = Integer.parseInt(request.require("reducer"));
        int chunkLines = request.getInt("chunkLines", 2000);
        transferExecutor.submit(() -> {
            ReducerStore reducer = reducerStores.computeIfAbsent(jobId, k -> new ReducerStore());
            try {
                List<String> keys = reducer.sortedKeys();
                StringBuilder chunk = new StringBuilder(1 << 16);
                int lines = 0;
                long sent = 0;
                for (String key : keys) {
                    // Space separated rather than comma: real URLs contain
                    // commas (Wikipedia titles like Baldwin_II,_Count_of_Flanders)
                    // but never contain whitespace once normalized, so a comma
                    // separator would make the output ambiguous to parse.
                    chunk.append(key).append('\t')
                         .append(String.join(" ", reducer.sortedSources(key))).append('\n');
                    if (++lines >= chunkLines) {
                        sendChunk(jobId, reducerId, chunk.toString());
                        sent += lines;
                        chunk.setLength(0);
                        lines = 0;
                    }
                }
                if (lines > 0) {
                    sendChunk(jobId, reducerId, chunk.toString());
                    sent += lines;
                }
                pool.request(config.controller, Message.of(Verbs.REDUCE_DONE,
                        "node", config.nodeId, "job", jobId,
                        "reducer", Integer.toString(reducerId),
                        "keys", Long.toString(sent),
                        "edges", Long.toString(reducer.edgeCount()),
                        "maxFanIn", Long.toString(reducer.maxFanIn())));
                log.info("reducer %d finalized: %d keys, %d edges, max fan-in %d",
                        reducerId, keys.size(), reducer.edgeCount(), reducer.maxFanIn());
            } catch (IOException e) {
                log.error("reducer %d finalize failed: %s", reducerId, e.getMessage());
            }
        });
    }

    private void sendChunk(String jobId, int reducerId, String chunk) throws IOException {
        pool.request(config.controller, Message.of(Verbs.REDUCE_CHUNK,
                        "node", config.nodeId, "job", jobId, "reducer", Integer.toString(reducerId))
                .withBody(chunk)).orThrow();
    }

    static List<Endpoint> decodeReducers(String encoded) {
        List<Endpoint> reducers = new ArrayList<>();
        for (String item : encoded.split(";")) {
            if (item.isBlank()) continue;
            String[] parts = item.split(",", -1);
            reducers.add(new Endpoint(parts[1], Integer.parseInt(parts[2])));
        }
        return reducers;
    }

    public void awaitShutdown() throws InterruptedException {
        while (!closed) Thread.sleep(500);
    }

    @Override
    public void close() {
        closed = true;
        for (MapPipeline pipeline : runningTasks.values()) pipeline.cancel();
        heartbeatScheduler.shutdownNow();
        if (gossip != null) gossip.close();
        Connection connection = controlConnection;
        if (connection != null) connection.close();
        taskExecutor.shutdownNow();
        transferExecutor.shutdownNow();
        server.close();
        pool.close();
    }
}
