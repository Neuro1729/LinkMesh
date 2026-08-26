package linkmesh.proto;

import linkmesh.common.Log;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Warm connections per remote endpoint, so the shuffle path does not pay a TCP
 * handshake per batch. A borrowed connection belongs to one caller until
 * released, so requests and replies cannot interleave.
 */
public final class ConnectionPool implements AutoCloseable {
    private static final Log log = Log.of("pool");

    private final Map<Endpoint, Deque<Connection>> idle = new ConcurrentHashMap<>();
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final int maxIdlePerEndpoint;
    private final long maxIdleNanos;

    private final AtomicLong opened = new AtomicLong();
    private final AtomicLong reused = new AtomicLong();

    private volatile boolean closed;

    public ConnectionPool(int connectTimeoutMs, int readTimeoutMs) {
        this(connectTimeoutMs, readTimeoutMs, 8, TimeUnit.SECONDS.toNanos(60));
    }

    public ConnectionPool(int connectTimeoutMs, int readTimeoutMs, int maxIdlePerEndpoint, long maxIdleNanos) {
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.maxIdlePerEndpoint = maxIdlePerEndpoint;
        this.maxIdleNanos = maxIdleNanos;
    }

    public Connection borrow(Endpoint endpoint) throws IOException {
        if (closed) throw new IOException("connection pool is closed");
        Deque<Connection> queue = idle.get(endpoint);
        if (queue != null) {
            while (true) {
                Connection candidate;
                synchronized (queue) { candidate = queue.pollFirst(); }
                if (candidate == null) break;
                if (candidate.isUsable() && candidate.idleNanos() < maxIdleNanos) {
                    reused.incrementAndGet();
                    return candidate;
                }
                candidate.close();
            }
        }
        opened.incrementAndGet();
        return Connection.connect(endpoint, connectTimeoutMs, readTimeoutMs);
    }

    public void release(Connection connection) {
        if (connection == null) return;
        if (closed || !connection.isUsable()) { connection.close(); return; }
        Deque<Connection> queue = idle.computeIfAbsent(connection.remote(), k -> new ArrayDeque<>());
        synchronized (queue) {
            if (queue.size() >= maxIdlePerEndpoint) { connection.close(); return; }
            queue.addFirst(connection);
        }
    }

    public void invalidate(Connection connection) {
        if (connection != null) connection.close();
    }

    /** Borrow, exchange one request/reply, release. Retries once if a pooled connection was stale. */
    public Message request(Endpoint endpoint, Message message) throws IOException {
        IOException firstFailure = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            Connection connection = borrow(endpoint);
            try {
                Message reply = connection.request(message);
                release(connection);
                return reply;
            } catch (IOException e) {
                invalidate(connection);
                if (firstFailure == null) firstFailure = e;
                log.debug("request %s to %s failed (attempt %d): %s", message.verb(), endpoint, attempt + 1, e.getMessage());
            }
        }
        throw firstFailure;
    }

    /** Sends a message and ignores the reply body, but still waits for the ack. */
    public void send(Endpoint endpoint, Message message) throws IOException {
        request(endpoint, message).orThrow();
    }

    /** Drops every pooled connection to an endpoint, used when a node is declared dead. */
    public void evict(Endpoint endpoint) {
        Deque<Connection> queue = idle.remove(endpoint);
        if (queue == null) return;
        synchronized (queue) {
            for (Connection connection : queue) connection.close();
            queue.clear();
        }
    }

    public long connectionsOpened() { return opened.get(); }

    public long connectionsReused() { return reused.get(); }

    @Override
    public void close() {
        closed = true;
        for (Endpoint endpoint : idle.keySet()) evict(endpoint);
    }
}
