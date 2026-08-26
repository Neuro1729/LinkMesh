package linkmesh.controller;

import linkmesh.cluster.ClusterState;
import linkmesh.cluster.NodeInfo;
import linkmesh.cluster.Placement;
import linkmesh.common.Log;
import linkmesh.proto.ConnectionPool;
import linkmesh.proto.Endpoint;
import linkmesh.proto.Message;
import linkmesh.proto.Verbs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Task placement and straggler handling.
 *
 * Placement prefers a node that already holds the partition. If every holder is
 * busy, an idle node takes the task and fetches the data first.
 *
 * Speculation: once enough tasks have finished to give a median, anything
 * running well past it gets a second attempt elsewhere and the first to finish
 * wins. No commit protocol is needed because the reducer stores edges in a Set,
 * so applying the same edge twice does nothing. A reduce that summed or counted
 * would need the loser fenced off first.
 */
public final class JobScheduler {
    private static final Log log = Log.of("sched");

    public record ReducerAssignment(int reducerId, String nodeId, Endpoint endpoint) {}

    public static final class Tuning {
        public double speculativeMultiplier = 1.5;
        public long minSpeculativeMillis = 5_000;
        public double speculativeStartFraction = 0.1;
        public int maxSpeculativeAttempts = Integer.MAX_VALUE;
        public boolean allowRemoteFetch = true;
        public int parserThreads = 0;
        public int queueCapacity = 0;
        public int shuffleBatch = 0;
        public int parseDelayMillis = 0;
    }

    private final JobState job;
    private final ClusterState cluster;
    private final Placement placement;
    private final ConnectionPool pool;
    private final List<ReducerAssignment> reducers;
    private final Tuning tuning;
    private final String encodedReducers;

    public JobScheduler(JobState job, ClusterState cluster, Placement placement, ConnectionPool pool,
                        List<ReducerAssignment> reducers, Tuning tuning) {
        this.job = job;
        this.cluster = cluster;
        this.placement = placement;
        this.pool = pool;
        this.reducers = List.copyOf(reducers);
        this.tuning = tuning;
        this.encodedReducers = encodeReducers(reducers);
    }

    private static String encodeReducers(List<ReducerAssignment> reducers) {
        StringBuilder sb = new StringBuilder();
        for (ReducerAssignment reducer : reducers) {
            if (!sb.isEmpty()) sb.append(';');
            sb.append(reducer.reducerId()).append(',')
              .append(reducer.endpoint().host()).append(',')
              .append(reducer.endpoint().port());
        }
        return sb.toString();
    }

    /** One scheduling pass. Cheap enough to run several times a second. */
    public void tick() {
        if (job.isComplete() || job.isCancelled()) return;
        assignPending();
        considerSpeculation();
    }

    private void assignPending() {
        for (String partition : job.unstartedPartitions()) {
            List<String> holders = placement.aliveHolders(partition, cluster);
            if (holders.isEmpty()) {
                job.cancel("partition " + partition + " has no live replica");
                log.error("no live replica of %s remains, job cannot finish", partition);
                return;
            }

            // Only nodes that have confirmed the bytes are on disk can run or
            // serve this partition. A replica still being transferred counts
            // toward "not lost" above, but sending a task there would fail.
            List<String> ready = placement.confirmedAliveHolders(partition, cluster);
            if (ready.isEmpty()) continue;

            NodeInfo target = pickIdle(ready);
            if (target != null) {
                launch(partition, target, null, false);
                continue;
            }
            if (!tuning.allowRemoteFetch) continue;

            NodeInfo borrower = pickAnyIdleExcluding(ready);
            if (borrower == null) continue;
            NodeInfo source = cluster.get(ready.get(0));
            if (source == null) continue;
            log.info("no idle holder for %s, %s will fetch it from %s", partition, borrower.id, source.id);
            launch(partition, borrower, source, false);
        }
    }

    private void considerSpeculation() {
        int completed = job.completedCount();
        int minimumSamples = Math.max(2, (int) Math.ceil(job.totalCount() * tuning.speculativeStartFraction));
        if (completed < minimumSamples) return;
        if (job.speculativeAttemptCount() >= tuning.maxSpeculativeAttempts) return;

        long median = job.medianCompletedMillis();
        if (median <= 0) return;
        long threshold = Math.max(tuning.minSpeculativeMillis, (long) (median * tuning.speculativeMultiplier));

        for (TaskAttempt attempt : job.allRunningAttempts()) {
            if (job.isDone(attempt.partition)) continue;
            if (attempt.elapsedMillis() <= threshold) continue;
            if (job.runningAttempts(attempt.partition).size() > 1) continue;

            List<String> ready = placement.confirmedAliveHolders(attempt.partition, cluster);
            if (ready.isEmpty()) continue;

            NodeInfo backup = pickBackupNode(attempt, ready);
            if (backup == null) continue;

            NodeInfo source = null;
            if (!ready.contains(backup.id)) {
                source = cluster.get(ready.get(0));
                if (source == null) continue;
            }
            log.info("straggler: %s on %s at %d ms vs median %d ms, starting backup on %s",
                    attempt.partition, attempt.nodeId, attempt.elapsedMillis(), median, backup.id);
            launch(attempt.partition, backup, source, true);
        }
    }

    /** Prefers another node that already holds the data, then any idle node. */
    private NodeInfo pickBackupNode(TaskAttempt attempt, List<String> readyHolders) {
        List<String> others = new ArrayList<>(readyHolders);
        others.remove(attempt.nodeId);
        NodeInfo target = pickIdle(others);
        if (target != null) return target;
        if (!tuning.allowRemoteFetch) return null;
        return pickAnyIdleExcluding(List.of(attempt.nodeId));
    }

    private NodeInfo pickIdle(List<String> nodeIds) {
        NodeInfo best = null;
        for (String nodeId : nodeIds) {
            NodeInfo node = cluster.get(nodeId);
            if (node == null || !node.hasFreeSlot()) continue;
            if (best == null || node.runningTasks() < best.runningTasks()) best = node;
        }
        return best;
    }

    private NodeInfo pickAnyIdleExcluding(java.util.Collection<String> exclude) {
        List<NodeInfo> candidates = new ArrayList<>();
        for (NodeInfo node : cluster.alive()) {
            if (exclude.contains(node.id) || !node.hasFreeSlot()) continue;
            candidates.add(node);
        }
        candidates.sort(Comparator.comparingInt(NodeInfo::runningTasks).thenComparing(n -> n.id));
        return candidates.isEmpty() ? null : candidates.get(0);
    }

    private void launch(String partition, NodeInfo node, NodeInfo source, boolean speculative) {
        TaskAttempt attempt = job.newAttempt(partition, node.id, speculative);
        node.taskStarted();

        Message request = Message.of(Verbs.RUN_TASK,
                "job", job.jobId,
                "attempt", attempt.attemptId,
                "partition", partition,
                "reducers", encodedReducers);
        if (source != null) {
            request = request.with("sourceHost", source.endpoint.host())
                             .with("sourcePort", Integer.toString(source.endpoint.port()));
        }
        if (tuning.parserThreads > 0) request = request.with("parserThreads", Integer.toString(tuning.parserThreads));
        if (tuning.queueCapacity > 0) request = request.with("queueCapacity", Integer.toString(tuning.queueCapacity));
        if (tuning.shuffleBatch > 0) request = request.with("shuffleBatch", Integer.toString(tuning.shuffleBatch));
        if (tuning.parseDelayMillis > 0) request = request.with("parseDelay", Integer.toString(tuning.parseDelayMillis));

        try {
            pool.request(node.endpoint, request).orThrow();
            log.info("assigned %s -> %s%s", partition, node.id,
                    speculative ? " (backup attempt)" : source != null ? " (remote fetch)" : " (local)");
        } catch (IOException | RuntimeException e) {
            log.warn("could not start %s on %s: %s", partition, node.id, e.getMessage());
            attempt.fail("dispatch failed: " + e.getMessage());
            node.taskFinished();
        }
    }

    public void onTaskDone(Message message) {
        String attemptId = message.require("attempt");
        String partition = message.require("partition");
        TaskAttempt attempt = job.findAttempt(attemptId);
        if (attempt == null) {
            log.warn("completion for unknown attempt %s", attemptId);
            return;
        }
        attempt.succeed(
                message.getLong("files", 0),
                message.getLong("pages", 0),
                message.getLong("links", 0),
                message.getLong("producerWaits", 0),
                message.getInt("queueHighWater", 0),
                message.getLong("batches", 0));
        releaseSlot(attempt.nodeId);

        long elapsed = message.getLong("elapsed", attempt.elapsedMillis());
        if (job.completePartition(partition, elapsed)) {
            log.info("%s complete on %s in %d ms (%d pages, %d links) [%d/%d]",
                    partition, attempt.nodeId, elapsed, attempt.pages(), attempt.links(),
                    job.completedCount(), job.totalCount());
            if (attempt.speculative) {
                log.info("backup attempt won for %s, cancelling the original", partition);
            }
            cancelOtherAttempts(partition, attemptId);
        } else {
            // A duplicate finish. Harmless because the reduce is idempotent, but
            // its metrics are dropped so files and links are not double counted.
            log.info("%s already complete, discarding duplicate result from %s", partition, attempt.nodeId);
        }
    }

    public void onTaskFailed(Message message) {
        String attemptId = message.require("attempt");
        String partition = message.require("partition");
        String reason = message.get("reason", "unknown");
        TaskAttempt attempt = job.findAttempt(attemptId);
        if (attempt != null) {
            if ("cancelled".equals(reason)) attempt.cancel(); else attempt.fail(reason);
            releaseSlot(attempt.nodeId);
        }
        if (job.isDone(partition)) return;
        if (!job.runningAttempts(partition).isEmpty()) return;
        log.warn("%s failed on %s (%s), will be rescheduled", partition,
                attempt == null ? "?" : attempt.nodeId, reason);
    }

    /** Fails every attempt that was running on a node that just died. */
    public void onNodeLost(String nodeId) {
        for (TaskAttempt attempt : job.allRunningAttempts()) {
            if (!attempt.nodeId.equals(nodeId)) continue;
            attempt.fail("node lost");
            if (!job.isDone(attempt.partition) && job.runningAttempts(attempt.partition).isEmpty()) {
                log.warn("%s will be rescheduled, lost with node %s", attempt.partition, nodeId);
            }
        }
    }

    private void cancelOtherAttempts(String partition, String winningAttemptId) {
        for (TaskAttempt other : job.runningAttempts(partition)) {
            if (other.attemptId.equals(winningAttemptId)) continue;
            NodeInfo node = cluster.get(other.nodeId);
            if (node == null) continue;
            try {
                pool.request(node.endpoint, Message.of(Verbs.CANCEL_TASK,
                        "job", job.jobId, "attempt", other.attemptId));
                log.info("cancelled losing attempt %s on %s", other.attemptId, other.nodeId);
            } catch (IOException e) {
                log.debug("could not cancel %s on %s: %s", other.attemptId, other.nodeId, e.getMessage());
            }
        }
    }

    private void releaseSlot(String nodeId) {
        NodeInfo node = cluster.get(nodeId);
        if (node != null) node.taskFinished();
    }

    public List<ReducerAssignment> reducers() { return reducers; }
}
