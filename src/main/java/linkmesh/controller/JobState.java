package linkmesh.controller;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bookkeeping for one running job: which partitions are left, which attempts are
 * in flight, and the timing history the speculator uses to spot a straggler.
 */
public final class JobState {

    public final String jobId;
    public final List<String> partitions;

    private final Set<String> completed = ConcurrentHashMap.newKeySet();
    private final Map<String, List<TaskAttempt>> attempts = new ConcurrentHashMap<>();
    private final List<Long> completedDurations = new CopyOnWriteArrayList<>();
    private final AtomicInteger attemptCounter = new AtomicInteger();

    private volatile boolean cancelled;
    private volatile String failure;

    public JobState(String jobId, List<String> partitions) {
        this.jobId = jobId;
        this.partitions = List.copyOf(partitions);
    }

    public boolean isComplete() { return completed.size() >= partitions.size(); }

    public boolean isCancelled() { return cancelled; }

    public void cancel(String reason) {
        this.cancelled = true;
        if (this.failure == null) this.failure = reason;
    }

    public String failure() { return failure; }

    public int completedCount() { return completed.size(); }

    public int totalCount() { return partitions.size(); }

    public boolean isDone(String partition) { return completed.contains(partition); }

    /**
     * Partitions that still need an attempt started, in stable order.
     *
     * Derived rather than tracked in a separate queue. A partition is eligible
     * whenever it is not finished and has no live attempt, so a failed,
     * cancelled, or node-lost attempt makes its partition schedulable again by
     * that fact alone -- there is no second copy of this state to fall out of
     * step with the attempt records.
     */
    public List<String> unstartedPartitions() {
        List<String> result = new ArrayList<>();
        for (String partition : partitions) {
            if (completed.contains(partition)) continue;
            if (runningAttempts(partition).isEmpty()) result.add(partition);
        }
        return result;
    }

    public List<TaskAttempt> runningAttempts(String partition) {
        List<TaskAttempt> result = new ArrayList<>();
        for (TaskAttempt attempt : attempts.getOrDefault(partition, List.of())) {
            if (attempt.isRunning()) result.add(attempt);
        }
        return result;
    }

    public List<TaskAttempt> allRunningAttempts() {
        List<TaskAttempt> result = new ArrayList<>();
        for (List<TaskAttempt> list : attempts.values()) {
            for (TaskAttempt attempt : list) if (attempt.isRunning()) result.add(attempt);
        }
        result.sort(Comparator.comparingLong(a -> a.startNanos));
        return result;
    }

    public TaskAttempt newAttempt(String partition, String nodeId, boolean speculative) {
        String attemptId = partition + "#" + attemptCounter.incrementAndGet();
        TaskAttempt attempt = new TaskAttempt(attemptId, partition, nodeId, speculative);
        attempts.computeIfAbsent(partition, k -> new CopyOnWriteArrayList<>()).add(attempt);
        return attempt;
    }

    public TaskAttempt findAttempt(String attemptId) {
        for (List<TaskAttempt> list : attempts.values()) {
            for (TaskAttempt attempt : list) {
                if (attempt.attemptId.equals(attemptId)) return attempt;
            }
        }
        return null;
    }

    /** Marks a partition done. Returns false if another attempt already won the race. */
    public boolean completePartition(String partition, long durationMillis) {
        boolean first = completed.add(partition);
        if (first) completedDurations.add(durationMillis);
        return first;
    }

    /**
     * Median duration of completed attempts. The speculator compares against the
     * median rather than the mean so one pathological straggler cannot inflate
     * the threshold and mask the next one.
     */
    public long medianCompletedMillis() {
        List<Long> sorted = new ArrayList<>(completedDurations);
        if (sorted.isEmpty()) return 0;
        sorted.sort(Long::compare);
        int middle = sorted.size() / 2;
        return sorted.size() % 2 == 1
                ? sorted.get(middle)
                : (sorted.get(middle - 1) + sorted.get(middle)) / 2;
    }

    public long totalPages() {
        long total = 0;
        for (List<TaskAttempt> list : attempts.values()) {
            for (TaskAttempt attempt : list) {
                if (attempt.state() == TaskAttempt.State.DONE) { total += attempt.pages(); break; }
            }
        }
        return total;
    }

    public long totalFiles() {
        long total = 0;
        for (List<TaskAttempt> list : attempts.values()) {
            for (TaskAttempt attempt : list) {
                if (attempt.state() == TaskAttempt.State.DONE) { total += attempt.files(); break; }
            }
        }
        return total;
    }

    public long totalLinks() {
        long total = 0;
        for (List<TaskAttempt> list : attempts.values()) {
            for (TaskAttempt attempt : list) {
                if (attempt.state() == TaskAttempt.State.DONE) { total += attempt.links(); break; }
            }
        }
        return total;
    }

    public int speculativeAttemptCount() {
        int count = 0;
        for (List<TaskAttempt> list : attempts.values()) {
            for (TaskAttempt attempt : list) if (attempt.speculative) count++;
        }
        return count;
    }

    public int speculativeWins() {
        int wins = 0;
        for (List<TaskAttempt> list : attempts.values()) {
            for (TaskAttempt attempt : list) {
                if (attempt.speculative && attempt.state() == TaskAttempt.State.DONE) wins++;
            }
        }
        return wins;
    }
}
