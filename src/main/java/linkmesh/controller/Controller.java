package linkmesh.controller;

import linkmesh.cluster.*;
import linkmesh.common.Log;
import linkmesh.common.Text;
import linkmesh.proto.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Membership, replica placement, job scheduling, and output collection.
 *
 * Holds no data itself. Partitions live on the nodes and their inventories are
 * re-read from them, so a controller restart loses nothing and the placement map
 * cannot drift away from what is actually on disk.
 *
 * Still a single point of failure for scheduling. Data survives it, running jobs
 * do not.
 */
public final class Controller implements AutoCloseable {
    private static final Log log = Log.of("control");

    public static final class Config {
        public int port = 9000;
        public int replicationFactor = 2;
        public int reducerCount = 2;
        public Path outputPath = Path.of("output/backlinks.tsv");
        public long suspectMillis = 3000;
        public long deadMillis = 8000;
        public long maintenanceMillis = 500;
        public long inventoryRefreshMillis = 2000;
        public int minWorkers = 1;
        public JobScheduler.Tuning tuning = new JobScheduler.Tuning();
    }

    public enum Phase { IDLE, MAP, REDUCE, DONE, FAILED }

    private final Config config;
    private final ClusterState cluster;
    private final Placement placement = new Placement();
    private final PlacementPlanner planner;
    private final ConnectionPool pool = new ConnectionPool(3000, 30_000);
    private final MessageServer server;
    private final ScheduledExecutorService maintenance =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "maintenance");
                thread.setDaemon(true);
                return thread;
            });
    private final ExecutorService background = Executors.newVirtualThreadPerTaskExecutor();

    private final Set<String> inFlightReplications = ConcurrentHashMap.newKeySet();

    // Recovery accounting. "Survived a node failure" is a weak claim on its own;
    // these turn it into how fast it was noticed, how much work had to be redone,
    // and how long the cluster ran below its replication factor.
    private final AtomicLong nodeFailures = new AtomicLong();
    private final AtomicLong lastContactMillis = new AtomicLong(-1);
    private final AtomicLong replicationsIssued = new AtomicLong();
    private final AtomicLong recoveryMillis = new AtomicLong(-1);
    private volatile long degradedSinceNanos = 0;
    private final AtomicLong outputLines = new AtomicLong();
    private final CountDownLatch jobFinished = new CountDownLatch(1);
    private final Object outputLock = new Object();

    private volatile JobState job;
    private volatile JobScheduler scheduler;
    private volatile BufferedWriter output;
    private volatile Phase phase = Phase.IDLE;
    private volatile long mapStageMillis;
    private volatile long jobStartNanos;
    private final Set<Integer> reducersFinished = ConcurrentHashMap.newKeySet();
    private final Map<Integer, long[]> reducerStats = new ConcurrentHashMap<>();
    private volatile boolean closed;

    public Controller(Config config) throws IOException {
        this.config = config;
        this.cluster = new ClusterState(config.suspectMillis, config.deadMillis);
        this.planner = new PlacementPlanner(config.replicationFactor);
        this.server = new MessageServer("controller", config.port, new Dispatcher());

        cluster.addListener(new ClusterState.MembershipListener() {
            @Override
            public void onStatusChange(NodeInfo node, NodeStatus previous, NodeStatus current) {
                if (current == NodeStatus.DEAD) handleNodeDeath(node);
            }
        });
    }

    public int start() throws IOException {
        int boundPort = server.start();
        log.info("controller listening on port %d (replicationFactor=%d, reducers=%d)",
                boundPort, config.replicationFactor, config.reducerCount);
        maintenance.scheduleWithFixedDelay(this::maintain, config.maintenanceMillis,
                config.maintenanceMillis, TimeUnit.MILLISECONDS);
        maintenance.scheduleWithFixedDelay(this::refreshInventories, 1000,
                config.inventoryRefreshMillis, TimeUnit.MILLISECONDS);
        return boundPort;
    }

    public ClusterState cluster() { return cluster; }

    public Placement placement() { return placement; }

    public boolean awaitJob(long timeout, TimeUnit unit) throws InterruptedException {
        return jobFinished.await(timeout, unit);
    }

    public Phase phase() { return phase; }

    // ---------------------------------------------------------------- messages

    private final class Dispatcher implements MessageServer.Handler {
        @Override
        public Message handle(Message request, Connection connection) throws Exception {
            return dispatch(request, connection);
        }

        @Override
        public void onDisconnect(Connection connection) {
            Object attachment = connection.attachment();
            if (attachment instanceof String nodeId) {
                // The control connection dropped. For a process that exited this
                // is instant and unambiguous, so there is no reason to wait out
                // the gossip timeout before reacting.
                cluster.markDead(nodeId, "control connection closed");
            }
        }
    }

    private Message dispatch(Message request, Connection connection) throws Exception {
        return switch (request.verb()) {
            case Verbs.HELLO -> {
                String nodeId = request.require("node");
                Endpoint endpoint = new Endpoint(request.require("host"), Integer.parseInt(request.require("port")));
                NodeInfo node = cluster.register(nodeId, endpoint);
                node.setMapSlots(request.getInt("slots", 1));
                node.setRole(NodeRole.parse(request.get("role")));
                node.setStoredBytes(request.getLong("bytes", 0));
                connection.attach(nodeId);

                Set<String> inventory = parseInventory(request.bodyText());
                node.replaceInventory(inventory);
                placement.setInventory(nodeId, inventory);
                log.info("node %s joined as %s with %d partitions (%s)", nodeId, node.role(),
                        inventory.size(), Text.humanBytes(node.storedBytes()));
                yield Message.ok("node", nodeId).withBody(cluster.encodeMembership());
            }
            case Verbs.HEARTBEAT -> {
                cluster.heartbeat(request.require("node"), request.getLong("counter", 0));
                NodeInfo node = cluster.get(request.require("node"));
                if (node != null) node.setStoredBytes(request.getLong("bytes", node.storedBytes()));
                yield Message.ok().withBody(cluster.encodeMembership());
            }
            case Verbs.NODE_STATUS -> {
                cluster.applyPeerReport(request.get("from", "?"), request.require("node"),
                        NodeStatus.valueOf(request.require("status")));
                yield Message.ok();
            }
            case Verbs.TASK_DONE -> {
                JobScheduler current = scheduler;
                if (current != null) current.onTaskDone(request);
                yield Message.ok();
            }
            case Verbs.TASK_FAILED -> {
                JobScheduler current = scheduler;
                if (current != null) current.onTaskFailed(request);
                yield Message.ok();
            }
            case Verbs.REDUCE_CHUNK -> {
                writeOutput(request.bodyText());
                yield Message.ok();
            }
            case Verbs.REDUCE_DONE -> {
                int reducerId = Integer.parseInt(request.require("reducer"));
                reducerStats.put(reducerId, new long[]{
                        request.getLong("keys", 0),
                        request.getLong("edges", 0),
                        request.getLong("maxFanIn", 0)});
                reducersFinished.add(reducerId);
                log.info("reducer %d finished: %s keys, %s edges, max fan-in %s",
                        reducerId, request.get("keys", "?"), request.get("edges", "?"),
                        request.get("maxFanIn", "?"));
                yield Message.ok();
            }
            case Verbs.PLACE -> {
                String partition = request.require("partition");
                List<String> targets = planner.chooseInitialHolders(placement, cluster, partition);
                StringBuilder sb = new StringBuilder();
                for (String nodeId : targets) {
                    NodeInfo node = cluster.get(nodeId);
                    if (node == null) continue;
                    if (!sb.isEmpty()) sb.append(';');
                    sb.append(nodeId).append(',').append(node.endpoint.host())
                      .append(',').append(node.endpoint.port());
                    // Recorded immediately so the next PLACE call spreads the
                    // following partition somewhere else instead of piling up.
                    placement.add(partition, nodeId);
                }
                yield Message.ok("count", Integer.toString(targets.size())).withBody(sb.toString());
            }
            case Verbs.SUBMIT -> {
                String jobId = request.get("job", "job-" + System.currentTimeMillis());
                startJob(jobId);
                yield Message.ok("job", jobId, "partitions", Integer.toString(job == null ? 0 : job.totalCount()));
            }
            case Verbs.CANCEL_JOB -> {
                JobState current = job;
                if (current != null && current.jobId.equals(request.get("job", current.jobId))) {
                    current.cancel("cancelled by client");
                    log.warn("job %s cancelled by client", current.jobId);
                }
                yield Message.ok();
            }
            case Verbs.STATUS -> Message.ok().withBody(statusReport());
            default -> Message.error("unknown verb " + request.verb());
        };
    }

    private static Set<String> parseInventory(String body) {
        if (body == null || body.isBlank()) return Set.of();
        Set<String> ids = new LinkedHashSet<>();
        for (String id : body.split(",")) {
            if (!id.isBlank()) ids.add(id.trim());
        }
        return ids;
    }

    // ------------------------------------------------------------ maintenance

    private void maintain() {
        if (closed) return;
        try {
            for (NodeInfo dead : cluster.sweep()) handleNodeDeath(dead);
            applyPlacementPlan();
            checkRecovered();
            driveJob();
        } catch (RuntimeException e) {
            log.warn("maintenance pass failed: %s: %s", e.getClass().getSimpleName(), e.getMessage());
        }
    }

    /**
     * Rebuilds placement from what nodes actually report holding.
     *
     * Deriving the map from ground truth rather than incrementing counters means
     * an interrupted transfer, a manual file deletion, or a node that rejoins
     * with a partial store all self-correct on the next pass.
     */
    private void refreshInventories() {
        if (closed) return;
        for (NodeInfo node : cluster.alive()) {
            background.submit(() -> {
                try {
                    Message reply = pool.request(node.endpoint, Message.of(Verbs.STORE_LIST));
                    if (reply.isError()) return;
                    Set<String> inventory = parseInventory(reply.bodyText());
                    node.replaceInventory(inventory);
                    placement.setInventory(node.id, inventory);
                } catch (IOException e) {
                    log.debug("inventory refresh failed for %s: %s", node.id, e.getMessage());
                }
            });
        }
    }

    private void applyPlacementPlan() {
        if (cluster.aliveCount() == 0) return;
        List<PlacementPlanner.Move> moves = planner.plan(placement, cluster);
        for (PlacementPlanner.Move move : moves) {
            String key = move.kind() + ":" + move.partition() + ":" + move.target();
            if (!inFlightReplications.add(key)) continue;
            replicationsIssued.incrementAndGet();
            background.submit(() -> {
                try {
                    if (move.kind() == PlacementPlanner.Kind.REPLICATE) {
                        executeReplicate(move);
                    } else {
                        executeDrop(move);
                    }
                } finally {
                    inFlightReplications.remove(key);
                }
            });
        }
    }

    private void executeReplicate(PlacementPlanner.Move move) {
        NodeInfo source = cluster.get(move.source());
        NodeInfo target = cluster.get(move.target());
        if (source == null || target == null || !source.isAlive() || !target.isAlive()) return;
        try {
            log.info("replicating %s from %s to %s", move.partition(), source.id, target.id);
            pool.request(source.endpoint, Message.of(Verbs.REPLICATE,
                    "partition", move.partition(),
                    "targetHost", target.endpoint.host(),
                    "targetPort", Integer.toString(target.endpoint.port()))).orThrow();
            placement.add(move.partition(), target.id);
        } catch (IOException | RuntimeException e) {
            log.warn("replication of %s to %s failed: %s", move.partition(), target.id, e.getMessage());
        }
    }

    private void executeDrop(PlacementPlanner.Move move) {
        NodeInfo node = cluster.get(move.source());
        if (node == null || !node.isAlive()) return;
        try {
            pool.request(node.endpoint, Message.of(Verbs.STORE_DROP, "partition", move.partition())).orThrow();
            placement.remove(move.partition(), node.id);
            log.info("dropped surplus replica of %s from %s", move.partition(), node.id);
        } catch (IOException | RuntimeException e) {
            log.debug("drop of %s on %s failed: %s", move.partition(), node.id, e.getMessage());
        }
    }

    private void handleNodeDeath(NodeInfo node) {
        nodeFailures.incrementAndGet();
        // How stale the last contact was when the node was declared dead. This
        // is not detection latency: when the control connection drops the
        // declaration is immediate, and this just shows how long ago the last
        // heartbeat happened to be. It matters for the other path, where a node
        // goes quiet with its socket still open and this climbs toward deadMillis.
        lastContactMillis.set(node.silentMillis());
        pool.evict(node.endpoint);
        JobScheduler current = scheduler;
        if (current != null) current.onNodeLost(node.id);
        placement.removeNode(node.id);
        List<String> lost = placement.lostPartitions(cluster);
        if (!lost.isEmpty()) {
            log.error("no surviving replica for %d partition(s): %s", lost.size(),
                    lost.size() > 5 ? lost.subList(0, 5) + "..." : lost);
        } else {
            log.info("node %s lost, its partitions remain available on other replicas", node.id);
        }
        if (degradedSinceNanos == 0) degradedSinceNanos = System.nanoTime();
    }

    /**
     * Closes the degraded window once every partition is back to full
     * replication, so recovery time covers the actual copying rather than just
     * the moment the loss was noticed.
     */
    private void checkRecovered() {
        if (degradedSinceNanos == 0) return;
        int factor = Math.min(planner.replicationFactor(), cluster.aliveCount());
        for (String partition : placement.sortedPartitions()) {
            if (placement.confirmedAliveHolders(partition, cluster).size() < factor) return;
        }
        long millis = (System.nanoTime() - degradedSinceNanos) / 1_000_000L;
        degradedSinceNanos = 0;
        recoveryMillis.set(millis);
        log.info("cluster back to full replication after %d ms", millis);
    }

    // --------------------------------------------------------------- job flow

    public synchronized void startJob(String jobId) throws IOException {
        if (phase == Phase.MAP || phase == Phase.REDUCE) {
            throw new IllegalStateException("a job is already running");
        }
        List<NodeInfo> alive = cluster.alive();
        if (alive.size() < config.minWorkers) {
            throw new IllegalStateException("need at least " + config.minWorkers + " nodes, have " + alive.size());
        }
        List<String> partitions = placement.sortedPartitions();
        if (partitions.isEmpty()) throw new IllegalStateException("cluster holds no partitions, ingest data first");

        List<String> lost = placement.lostPartitions(cluster);
        if (!lost.isEmpty()) {
            throw new IllegalStateException("cannot start: " + lost.size() + " partitions have no live replica");
        }

        List<NodeInfo> reducerCandidates = chooseReducerCandidates(alive);
        int reducerCount = Math.min(config.reducerCount, reducerCandidates.size());
        List<JobScheduler.ReducerAssignment> reducers = new ArrayList<>();
        for (int i = 0; i < reducerCount; i++) {
            NodeInfo node = reducerCandidates.get(i);
            reducers.add(new JobScheduler.ReducerAssignment(i, node.id, node.endpoint));
        }

        Files.createDirectories(config.outputPath.toAbsolutePath().getParent());
        output = Files.newBufferedWriter(config.outputPath, StandardCharsets.UTF_8);
        outputLines.set(0);
        reducersFinished.clear();
        reducerStats.clear();

        job = new JobState(jobId, partitions);
        scheduler = new JobScheduler(job, cluster, placement, pool, reducers, config.tuning);
        jobStartNanos = System.nanoTime();
        phase = Phase.MAP;

        log.info("job %s started: %d partitions, %d nodes, %d reducers (%s)",
                jobId, partitions.size(), alive.size(), reducerCount, reducers);
    }

    /**
     * Orders nodes by how suitable they are to hold reducer state: machines that
     * asked for the job first, then unopinionated ones, and machines that opted
     * out only as a last resort.
     *
     * The fallback is deliberate rather than an error. Refusing to run because
     * every node said "mapper" would be a worse failure than running with a
     * warning, and the warning names the machine so the operator can fix the
     * configuration rather than guess.
     */
    private List<NodeInfo> chooseReducerCandidates(List<NodeInfo> alive) {
        List<NodeInfo> preferred = new ArrayList<>();
        List<NodeInfo> neutral = new ArrayList<>();
        List<NodeInfo> optedOut = new ArrayList<>();
        for (NodeInfo node : alive) {
            switch (node.role()) {
                case REDUCER -> preferred.add(node);
                case AUTO -> neutral.add(node);
                case MAPPER -> optedOut.add(node);
            }
        }
        List<NodeInfo> ordered = new ArrayList<>(preferred);
        ordered.addAll(neutral);
        if (ordered.size() < config.reducerCount && !optedOut.isEmpty()) {
            int shortfall = Math.min(config.reducerCount - ordered.size(), optedOut.size());
            for (int i = 0; i < shortfall; i++) {
                NodeInfo node = optedOut.get(i);
                log.warn("using mapper-only node %s as a reducer: only %d node(s) are reducer-eligible "
                        + "but --reducers is %d", node.id, ordered.size(), config.reducerCount);
                ordered.add(node);
            }
        }
        return ordered;
    }

    private void driveJob() {
        JobState current = job;
        JobScheduler currentScheduler = scheduler;
        if (current == null || currentScheduler == null) return;

        if (current.isCancelled() && phase != Phase.FAILED) {
            failJob(current.failure());
            return;
        }

        if (phase == Phase.MAP) {
            currentScheduler.tick();
            if (current.isComplete()) {
                mapStageMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - jobStartNanos);
                log.info("map stage complete in %d ms, finalizing %d reducers",
                        mapStageMillis, currentScheduler.reducers().size());
                phase = Phase.REDUCE;
                finalizeReducers(currentScheduler);
            }
        } else if (phase == Phase.REDUCE) {
            if (reducersFinished.size() >= currentScheduler.reducers().size()) completeJob(current, currentScheduler);
        }
    }

    private void finalizeReducers(JobScheduler currentScheduler) {
        for (JobScheduler.ReducerAssignment reducer : currentScheduler.reducers()) {
            background.submit(() -> {
                try {
                    pool.request(reducer.endpoint(), Message.of(Verbs.FINALIZE,
                            "job", job.jobId, "reducer", Integer.toString(reducer.reducerId()))).orThrow();
                } catch (IOException | RuntimeException e) {
                    log.error("could not finalize reducer %d on %s: %s",
                            reducer.reducerId(), reducer.nodeId(), e.getMessage());
                    job.cancel("reducer " + reducer.reducerId() + " finalize failed");
                }
            });
        }
    }

    private void completeJob(JobState current, JobScheduler currentScheduler) {
        long totalMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - jobStartNanos);
        closeOutput();
        phase = Phase.DONE;

        long keys = 0;
        long edges = 0;
        long maxFanIn = 0;
        for (long[] stats : reducerStats.values()) {
            keys += stats[0];
            edges += stats[1];
            maxFanIn = Math.max(maxFanIn, stats[2]);
        }

        log.info("job %s COMPLETE, output written to %s", current.jobId, config.outputPath.toAbsolutePath());
        Log.metric("total_job_ms", totalMillis);
        Log.metric("map_stage_ms", mapStageMillis);
        Log.metric("nodes", cluster.aliveCount());
        Log.metric("partitions", current.totalCount());
        Log.metric("reducers", currentScheduler.reducers().size());
        Log.metric("pages", current.totalPages());
        Log.metric("page_files", current.totalFiles());
        Log.metric("links_emitted", current.totalLinks());
        Log.metric("backlink_keys", keys);
        Log.metric("backlink_edges", edges);
        Log.metric("max_fan_in", maxFanIn);
        Log.metric("reduce_stage_ms", Math.max(0, totalMillis - mapStageMillis));
        Log.metric("tasks_local", current.localLaunches());
        Log.metric("tasks_fetched", current.fetchLaunches());
        Log.metric("locality_pct", String.format("%.1f", current.localityRate() * 100));
        Log.metric("node_failures", nodeFailures.get());
        if (lastContactMillis.get() >= 0) Log.metric("failure_last_contact_ms", lastContactMillis.get());
        Log.metric("tasks_rescheduled", current.rescheduledAfterLoss());
        Log.metric("replications_issued", replicationsIssued.get());
        if (recoveryMillis.get() >= 0) Log.metric("replication_recovery_ms", recoveryMillis.get());
        Log.metric("speculative_attempts", current.speculativeAttemptCount());
        Log.metric("speculative_wins", current.speculativeWins());
        Log.metric("connections_opened", pool.connectionsOpened());
        Log.metric("connections_reused", pool.connectionsReused());
        jobFinished.countDown();
    }

    private void failJob(String reason) {
        phase = Phase.FAILED;
        closeOutput();
        log.error("job failed: %s", reason);
        jobFinished.countDown();
    }

    private void writeOutput(String chunk) throws IOException {
        BufferedWriter writer = output;
        if (writer == null) return;
        synchronized (outputLock) {
            writer.write(chunk);
            long lines = chunk.chars().filter(c -> c == '\n').count();
            outputLines.addAndGet(lines);
        }
    }

    private void closeOutput() {
        synchronized (outputLock) {
            BufferedWriter writer = output;
            if (writer == null) return;
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                log.error("could not close output: %s", e.getMessage());
            }
            output = null;
        }
    }

    private String statusReport() {
        StringBuilder sb = new StringBuilder();
        sb.append("phase=").append(phase).append('\n');
        sb.append("nodes=").append(cluster.aliveCount()).append('\n');
        sb.append("partitions=").append(placement.partitionCount()).append('\n');
        sb.append("replicationFactor=").append(config.replicationFactor).append('\n');
        JobState current = job;
        if (current != null) {
            sb.append("job=").append(current.jobId).append('\n');
            sb.append("progress=").append(current.completedCount()).append('/').append(current.totalCount()).append('\n');
            sb.append("speculativeAttempts=").append(current.speculativeAttemptCount()).append('\n');
            sb.append("speculativeWins=").append(current.speculativeWins()).append('\n');
        }
        for (NodeInfo node : cluster.alive()) {
            sb.append("node\t").append(node.id).append('\t').append(node.endpoint).append('\t')
              .append(node.status()).append('\t').append(node.role()).append('\t')
              .append(placement.loadOf(node.id)).append(" partitions\t")
              .append(node.runningTasks()).append('/').append(node.mapSlots()).append(" tasks\t")
              .append(Text.humanBytes(node.storedBytes())).append('\n');
        }
        List<String> under = new ArrayList<>();
        for (String partition : placement.sortedPartitions()) {
            int holders = placement.aliveHolders(partition, cluster).size();
            if (holders < Math.min(config.replicationFactor, cluster.aliveCount())) {
                under.add(partition + "(" + holders + ")");
            }
        }
        sb.append("underReplicated=").append(under.size());
        if (!under.isEmpty()) sb.append('\t').append(under.size() > 10 ? under.subList(0, 10) + "..." : under);
        sb.append('\n');
        return sb.toString();
    }

    @Override
    public void close() {
        closed = true;
        maintenance.shutdownNow();
        background.shutdownNow();
        closeOutput();
        server.close();
        pool.close();
    }
}
