package linkmesh.worker;

import linkmesh.common.Hashing;
import linkmesh.common.Log;
import linkmesh.common.MapRecord;
import linkmesh.proto.ConnectionPool;
import linkmesh.proto.Endpoint;
import linkmesh.proto.Message;
import linkmesh.proto.Verbs;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Buffers edges per reducer and ships them in batches.
 *
 * Partitioning is hash(targetUrl) mod reducerCount, so every occurrence of a key
 * lands on one reducer and the reduce needs no second grouping pass.
 *
 * Each reducer has its own lock. The critical section only appends and swaps the
 * buffer; the socket write happens with no lock held, so parser threads do not
 * serialize behind one another and a slow reducer does not stall the rest.
 */
public final class ShuffleWriter implements AutoCloseable {
    private static final Log log = Log.of("shuffle");

    private final String jobId;
    private final List<Endpoint> reducers;
    private final ConnectionPool pool;
    private final int batchSize;

    private final List<MapRecord>[] buffers;
    private final Object[] locks;

    private final AtomicLong recordsSent = new AtomicLong();
    private final AtomicLong batchesSent = new AtomicLong();
    private final AtomicLong bytesSent = new AtomicLong();

    @SuppressWarnings("unchecked")
    public ShuffleWriter(String jobId, List<Endpoint> reducers, ConnectionPool pool, int batchSize) {
        if (reducers.isEmpty()) throw new IllegalArgumentException("no reducers configured");
        this.jobId = jobId;
        this.reducers = List.copyOf(reducers);
        this.pool = pool;
        this.batchSize = Math.max(1, batchSize);
        this.buffers = new List[this.reducers.size()];
        this.locks = new Object[this.reducers.size()];
        for (int i = 0; i < this.reducers.size(); i++) {
            buffers[i] = new ArrayList<>(this.batchSize);
            locks[i] = new Object();
        }
    }

    public void emit(MapRecord record) throws IOException {
        int reducerId = Hashing.bucket(record.targetUrl(), reducers.size());
        List<MapRecord> ready = null;
        synchronized (locks[reducerId]) {
            buffers[reducerId].add(record);
            if (buffers[reducerId].size() >= batchSize) {
                ready = buffers[reducerId];
                buffers[reducerId] = new ArrayList<>(batchSize);
            }
        }
        if (ready != null) send(reducerId, ready);
    }

    public void flushAll() throws IOException {
        for (int reducerId = 0; reducerId < reducers.size(); reducerId++) {
            List<MapRecord> ready;
            synchronized (locks[reducerId]) {
                if (buffers[reducerId].isEmpty()) continue;
                ready = buffers[reducerId];
                buffers[reducerId] = new ArrayList<>(batchSize);
            }
            send(reducerId, ready);
        }
    }

    private void send(int reducerId, List<MapRecord> batch) throws IOException {
        byte[] payload = encode(batch);
        Endpoint endpoint = reducers.get(reducerId);
        Message request = Message.of(Verbs.MAP_BATCH,
                        "job", jobId,
                        "reducer", Integer.toString(reducerId),
                        "records", Integer.toString(batch.size()))
                .withBody(payload);
        Message reply = pool.request(endpoint, request);
        if (reply.isError()) {
            throw new IOException("reducer " + reducerId + " rejected batch: " + reply.get("reason", "unknown"));
        }
        recordsSent.addAndGet(batch.size());
        batchesSent.incrementAndGet();
        bytesSent.addAndGet(payload.length);
        log.debug("sent %d records to reducer %d", batch.size(), reducerId);
    }

    /**
     * Length-prefixed binary rather than delimited text: a URL cannot break the
     * framing no matter what characters it contains, and it avoids the base64
     * inflation the old line-oriented protocol paid on every batch.
     */
    static byte[] encode(List<MapRecord> batch) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(batch.size() * 96);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(batch.size());
            for (MapRecord record : batch) {
                out.writeUTF(record.targetUrl());
                out.writeUTF(record.sourceUrl());
            }
        }
        return bytes.toByteArray();
    }

    public long recordsSent() { return recordsSent.get(); }

    public long batchesSent() { return batchesSent.get(); }

    public long bytesSent() { return bytesSent.get(); }

    @Override
    public void close() throws IOException {
        flushAll();
    }
}
