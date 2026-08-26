package linkmesh.worker;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bounded hand-off from the directory scanner to the parser pool.
 *
 * Built on ReentrantLock and two Conditions rather than ArrayBlockingQueue so
 * the backpressure is instrumented: producerWaits and highWaterMark let a run
 * show that the producer actually blocked.
 *
 * await() sits in while loops, not ifs. A signalled thread can lose the race to
 * another consumer, and spurious wakeups are allowed.
 */
public final class BoundedTaskQueue<T> {
    private final int capacity;
    private final Deque<T> queue = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();

    private boolean closed;
    private long producerWaits;
    private long consumerWaits;
    private long totalEnqueued;
    private int highWaterMark;

    public BoundedTaskQueue(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
        this.capacity = capacity;
    }

    /** Blocks while the queue is full. Returns false if the queue closed underneath us. */
    public boolean put(T value) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (!closed && queue.size() >= capacity) {
                producerWaits++;
                notFull.await();
            }
            if (closed) return false;
            queue.addLast(value);
            totalEnqueued++;
            if (queue.size() > highWaterMark) highWaterMark = queue.size();
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Blocks for an item. Returns null once the queue is closed and drained. */
    public T take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (queue.isEmpty() && !closed) {
                consumerWaits++;
                notEmpty.await();
            }
            if (queue.isEmpty()) return null;
            T value = queue.removeFirst();
            notFull.signal();
            return value;
        } finally {
            lock.unlock();
        }
    }

    /** Closes for new input and wakes everyone. Consumers still drain what is queued. */
    public void close() {
        lock.lock();
        try {
            closed = true;
            notEmpty.signalAll();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public boolean isClosed() {
        lock.lock();
        try { return closed; } finally { lock.unlock(); }
    }

    public int size() {
        lock.lock();
        try { return queue.size(); } finally { lock.unlock(); }
    }

    public long producerWaits() {
        lock.lock();
        try { return producerWaits; } finally { lock.unlock(); }
    }

    public long consumerWaits() {
        lock.lock();
        try { return consumerWaits; } finally { lock.unlock(); }
    }

    public long totalEnqueued() {
        lock.lock();
        try { return totalEnqueued; } finally { lock.unlock(); }
    }

    public int highWaterMark() {
        lock.lock();
        try { return highWaterMark; } finally { lock.unlock(); }
    }

    public int capacity() { return capacity; }
}
