package linkmesh.controller;

import java.util.concurrent.TimeUnit;

/**
 * One execution of one partition on one node.
 *
 * A partition can have several attempts alive at once: the original plus a
 * speculative backup started because the original looked like a straggler.
 * The first attempt to report success wins and the others are cancelled.
 */
public final class TaskAttempt {

    public enum State { RUNNING, DONE, FAILED, CANCELLED }

    public final String attemptId;
    public final String partition;
    public final String nodeId;
    public final boolean speculative;
    public final long startNanos;

    private volatile State state = State.RUNNING;
    private volatile long endNanos;
    private volatile long files;
    private volatile long pages;
    private volatile long links;
    private volatile long queueWaits;
    private volatile int queueHighWater;
    private volatile long batches;
    private volatile String failureReason;

    public TaskAttempt(String attemptId, String partition, String nodeId, boolean speculative) {
        this.attemptId = attemptId;
        this.partition = partition;
        this.nodeId = nodeId;
        this.speculative = speculative;
        this.startNanos = System.nanoTime();
    }

    public State state() { return state; }

    public boolean isRunning() { return state == State.RUNNING; }

    public long elapsedMillis() {
        long end = endNanos == 0 ? System.nanoTime() : endNanos;
        return TimeUnit.NANOSECONDS.toMillis(end - startNanos);
    }

    public void succeed(long files, long pages, long links, long queueWaits, int queueHighWater, long batches) {
        this.endNanos = System.nanoTime();
        this.files = files;
        this.pages = pages;
        this.links = links;
        this.queueWaits = queueWaits;
        this.queueHighWater = queueHighWater;
        this.batches = batches;
        this.state = State.DONE;
    }

    public void fail(String reason) {
        this.endNanos = System.nanoTime();
        this.failureReason = reason;
        this.state = State.FAILED;
    }

    public void cancel() {
        this.endNanos = System.nanoTime();
        this.state = State.CANCELLED;
    }

    public long files() { return files; }

    public long pages() { return pages; }

    public long links() { return links; }

    public long queueWaits() { return queueWaits; }

    public int queueHighWater() { return queueHighWater; }

    public long batches() { return batches; }

    public String failureReason() { return failureReason; }

    @Override
    public String toString() {
        return attemptId + " on " + nodeId + (speculative ? " (backup)" : "") + " " + state;
    }
}
