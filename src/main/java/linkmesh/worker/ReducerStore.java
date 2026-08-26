package linkmesh.worker;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory backlink index for one reducer and one job.
 *
 * Values are Sets, so applying the same edge twice is a no-op. That is what lets
 * speculative execution skip a commit protocol.
 *
 * Budget roughly 100-200 bytes of heap per unique edge. Past a few million edges
 * per reducer this needs to spill to disk instead.
 */
public final class ReducerStore {

    private final Map<String, Set<String>> index = new ConcurrentHashMap<>();

    /**
     * Canonical instance per distinct source URL.
     *
     * A page with 43 outbound links contributes its own URL to 43 different
     * target sets, and every batch decode allocates a fresh String for it. Left
     * alone, the store ends up holding one String object per edge rather than
     * one per page, which on a Wikipedia-sized corpus is the difference between
     * fitting in a 512 MB heap and not.
     */
    private final Map<String, String> sourcePool = new ConcurrentHashMap<>();

    private final AtomicLong recordsAccepted = new AtomicLong();
    private final AtomicLong batchesAccepted = new AtomicLong();

    /** Decodes and merges one shuffle batch. Safe to call from many mappers at once. */
    public int accept(byte[] payload) throws IOException {
        int applied = 0;
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                String target = in.readUTF();
                String source = in.readUTF();
                String canonical = sourcePool.putIfAbsent(source, source);
                index.computeIfAbsent(target, k -> ConcurrentHashMap.newKeySet())
                     .add(canonical == null ? source : canonical);
                applied++;
            }
        }
        recordsAccepted.addAndGet(applied);
        batchesAccepted.incrementAndGet();
        return applied;
    }

    public int distinctSources() { return sourcePool.size(); }

    public int keyCount() { return index.size(); }

    public long recordsAccepted() { return recordsAccepted.get(); }

    public long batchesAccepted() { return batchesAccepted.get(); }

    /** Total edges retained after set deduplication, the honest link count. */
    public long edgeCount() {
        long total = 0;
        for (Set<String> sources : index.values()) total += sources.size();
        return total;
    }

    /** Largest fan-in, the number to watch for reducer skew on real web data. */
    public long maxFanIn() {
        long max = 0;
        for (Set<String> sources : index.values()) max = Math.max(max, sources.size());
        return max;
    }

    public List<String> sortedKeys() {
        List<String> keys = new ArrayList<>(index.keySet());
        Collections.sort(keys);
        return keys;
    }

    public List<String> sortedSources(String key) {
        Set<String> sources = index.get(key);
        if (sources == null) return List.of();
        List<String> result = new ArrayList<>(sources);
        Collections.sort(result);
        return result;
    }

    public void clear() {
        index.clear();
        sourcePool.clear();
    }
}
