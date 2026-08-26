package linkmesh.storage;

import linkmesh.common.Log;
import linkmesh.common.Text;
import linkmesh.proto.Archive;
import linkmesh.proto.ProtocolException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * On-disk replica store for one node.
 *
 *   partitions/&lt;id&gt;/   page files
 *   meta/&lt;id&gt;.meta      size, file count, SHA-256
 *   tmp/                 staging for in-flight transfers
 *
 * Writes stage in tmp and only move into place once the digest checks out, so an
 * interrupted transfer cannot leave a partial partition that reads as complete.
 * The store survives restarts, so a node keeps its replicas across a bounce.
 */
public final class LocalStore {
    private static final Log log = Log.of("store");

    private final Path root;
    private final Path partitionsDir;
    private final Path metaDir;
    private final Path tmpDir;

    private final Map<String, PartitionMeta> inventory = new ConcurrentHashMap<>();
    private final Map<String, Object> locks = new ConcurrentHashMap<>();

    // Map tasks read a partition by path, not by open handle: the scan enumerates
    // Paths and the parser opens them much later. Anything that unlinks the
    // directory in between makes files vanish mid-scan. Readers register here so
    // drop and republish can leave a partition that is being read alone.
    private final Map<String, Integer> readers = new ConcurrentHashMap<>();

    public LocalStore(Path root) throws IOException {
        this.root = root.toAbsolutePath().normalize();
        this.partitionsDir = this.root.resolve("partitions");
        this.metaDir = this.root.resolve("meta");
        this.tmpDir = this.root.resolve("tmp");
        Files.createDirectories(partitionsDir);
        Files.createDirectories(metaDir);
        Files.createDirectories(tmpDir);
        clearTmp();
        reload();
    }

    public Path root() { return root; }

    /** Rebuilds the in-memory inventory from disk, dropping any orphaned entries. */
    public void reload() throws IOException {
        inventory.clear();
        try (Stream<Path> stream = Files.list(metaDir)) {
            for (Path metaFile : stream.filter(p -> p.toString().endsWith(".meta")).toList()) {
                try {
                    PartitionMeta meta = PartitionMeta.read(metaFile);
                    if (Files.isDirectory(partitionsDir.resolve(meta.id()))) {
                        inventory.put(meta.id(), meta);
                    } else {
                        log.warn("orphaned metadata for %s, removing", meta.id());
                        Files.deleteIfExists(metaFile);
                    }
                } catch (Exception e) {
                    log.warn("unreadable metadata %s: %s", metaFile.getFileName(), e.getMessage());
                }
            }
        }
        if (!inventory.isEmpty()) {
            log.info("loaded %d local partitions (%s)", inventory.size(), Text.humanBytes(totalBytes()));
        }
    }

    public Set<String> ids() { return Set.copyOf(inventory.keySet()); }

    public Map<String, PartitionMeta> inventory() { return Map.copyOf(inventory); }

    public boolean has(String id) { return inventory.containsKey(id); }

    public PartitionMeta meta(String id) { return inventory.get(id); }

    public int size() { return inventory.size(); }

    public long totalBytes() {
        long total = 0;
        for (PartitionMeta meta : inventory.values()) total += meta.bytes();
        return total;
    }

    /** Absolute path of a stored partition, for the map pipeline to scan. */
    public Path pathOf(String id) {
        PartitionMeta meta = inventory.get(id);
        if (meta == null) throw new IllegalStateException("partition not held locally: " + id);
        return partitionsDir.resolve(id);
    }

    /**
     * Pins a partition for reading. Returns false if it is not held, in which
     * case the caller has nothing to read. Every successful acquire must be
     * matched by a release, or the replica can never be reclaimed.
     */
    public boolean acquire(String id) {
        synchronized (lockFor(id)) {
            if (!inventory.containsKey(id)) return false;
            readers.merge(id, 1, Integer::sum);
            return true;
        }
    }

    public void release(String id) {
        synchronized (lockFor(id)) {
            readers.computeIfPresent(id, (key, count) -> count <= 1 ? null : count - 1);
        }
    }

    /** How many map tasks are currently reading this partition. */
    public int readerCount(String id) {
        return readers.getOrDefault(id, 0);
    }

    /** Streams a partition out as an archive, for replication to a peer. */
    public Archive.Written pack(String id, OutputStream sink) throws IOException {
        if (!has(id)) throw new IllegalStateException("partition not held locally: " + id);
        return Archive.pack(partitionsDir.resolve(id), sink);
    }

    /**
     * Unpacks an incoming archive into the store. If expectedSha is non-null and
     * does not match what arrived, the staged copy is discarded and nothing is
     * published, so a corrupted transfer never becomes a visible replica.
     */
    public PartitionMeta store(String id, InputStream archive, String expectedSha) throws IOException {
        validateId(id);
        synchronized (lockFor(id)) {
            // Partition content is immutable, so an incoming copy whose digest
            // matches what is already on disk has nothing to add. Skipping it is
            // not just an optimization: publishing replaces the directory, and
            // doing that under a map task currently reading the partition would
            // make its files vanish mid-scan. Duplicate pushes are routine after
            // a node dies, when re-replication and an on-demand task fetch can
            // both target the same node at once.
            PartitionMeta existing = inventory.get(id);
            if (existing != null && expectedSha != null && !expectedSha.isBlank()
                    && expectedSha.equals(existing.sha256())) {
                log.debug("already hold %s with matching digest, skipping rewrite", id);
                return existing;
            }
            // A digest that does not match should not happen, since partition
            // content is immutable once ingested. If it does, the copy already on
            // disk is the one a running task is reading, so keep it and let the
            // sender retry on a later tick rather than pulling the directory out
            // from under the reader.
            if (existing != null && readerCount(id) > 0) {
                log.warn("%s is being read by %d task(s), deferring rewrite", id, readerCount(id));
                return existing;
            }
            Path staging = tmpDir.resolve(id + "-" + UUID.randomUUID());
            try {
                Archive.Written written = Archive.unpack(archive, staging);
                if (expectedSha != null && !expectedSha.isBlank() && !expectedSha.equals(written.sha256())) {
                    throw new ProtocolException("checksum mismatch for " + id
                            + ": expected " + expectedSha + " got " + written.sha256());
                }
                return publish(id, staging, written);
            } catch (IOException | RuntimeException e) {
                deleteRecursively(staging);
                throw e;
            }
        }
    }

    /** Adopts a locally built directory, used by the ingester on the controller host. */
    public PartitionMeta storeLocalDirectory(String id, Path sourceDir) throws IOException {
        validateId(id);
        synchronized (lockFor(id)) {
            Path staging = tmpDir.resolve(id + "-" + UUID.randomUUID());
            Files.createDirectories(staging);
            try {
                copyTree(sourceDir, staging);
                Archive.Written written = Archive.pack(staging, OutputStream.nullOutputStream());
                return publish(id, staging, written);
            } catch (IOException | RuntimeException e) {
                deleteRecursively(staging);
                throw e;
            }
        }
    }

    private PartitionMeta publish(String id, Path staging, Archive.Written written) throws IOException {
        Path destination = partitionsDir.resolve(id);
        if (Files.exists(destination)) {
            // Rename the old copy out of the way rather than deleting in place.
            // Windows refuses to move onto an existing directory, and refuses to
            // delete one while any file inside it is open -- which a concurrently
            // running map task will do. Renaming is far more likely to succeed,
            // and readers holding open handles keep reading the retired copy
            // safely while the new one is published.
            Path retired = tmpDir.resolve(id + "-retired-" + UUID.randomUUID());
            try {
                // Left in tmp rather than deleted: a reader that already opened a
                // file keeps its handle valid, and clearTmp() reclaims the space on
                // the next start. Deleting here would unlink those files while they
                // are still being read.
                Files.move(destination, retired);
            } catch (IOException e) {
                deleteRecursively(destination);
            }
        }
        moveIntoPlace(staging, destination);
        PartitionMeta meta = new PartitionMeta(id, written.files(), written.bytes(),
                written.sha256(), System.currentTimeMillis());
        meta.write(metaDir.resolve(id + ".meta"));
        inventory.put(id, meta);
        log.debug("stored %s (%d files, %s)", id, written.files(), Text.humanBytes(written.bytes()));
        return meta;
    }

    /**
     * Removes a replica. Refuses while a map task is reading the partition: the
     * reader holds paths rather than handles, so deleting under it makes files
     * vanish mid-scan. The caller re-plans on a later tick, by which point the
     * task has finished and the surplus replica goes away then.
     */
    public boolean drop(String id) {
        synchronized (lockFor(id)) {
            if (readerCount(id) > 0) {
                log.info("not dropping %s, %d task(s) still reading it", id, readerCount(id));
                return false;
            }
            PartitionMeta removed = inventory.remove(id);
            if (removed == null) return false;
            deleteRecursively(partitionsDir.resolve(id));
            try {
                Files.deleteIfExists(metaDir.resolve(id + ".meta"));
            } catch (IOException e) {
                log.warn("could not remove metadata for %s: %s", id, e.getMessage());
            }
            log.info("dropped replica %s", id);
            return true;
        }
    }

    /**
     * Retries the final rename briefly. On Windows a directory move can fail
     * transiently while an indexer, a virus scanner, or a just-closed reader
     * still holds a handle, and those clear in milliseconds.
     */
    private static void moveIntoPlace(Path staging, Path destination) throws IOException {
        IOException lastFailure = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                try {
                    Files.move(staging, destination, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(staging, destination);
                }
                return;
            } catch (IOException e) {
                lastFailure = e;
                try {
                    Thread.sleep(50L * (attempt + 1));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        throw lastFailure;
    }

    private Object lockFor(String id) {
        return locks.computeIfAbsent(id, k -> new Object());
    }

    /** Partition ids become path segments, so anything path-like is rejected. */
    private void validateId(String id) {
        if (id == null || id.isBlank()) throw new IllegalArgumentException("blank partition id");
        if (!id.matches("[A-Za-z0-9._-]{1,128}")) {
            throw new IllegalArgumentException("illegal partition id: " + id);
        }
        if (id.equals(".") || id.equals("..")) {
            throw new IllegalArgumentException("illegal partition id: " + id);
        }
    }

    private void clearTmp() {
        try (Stream<Path> stream = Files.list(tmpDir)) {
            stream.forEach(LocalStore::deleteRecursively);
        } catch (IOException e) {
            log.warn("could not clear staging directory: %s", e.getMessage());
        }
    }

    private static void copyTree(Path source, Path target) throws IOException {
        try (Stream<Path> stream = Files.walk(source)) {
            stream.forEach(path -> {
                try {
                    Path destination = target.resolve(source.relativize(path).toString());
                    if (Files.isDirectory(path)) {
                        Files.createDirectories(destination);
                    } else {
                        Files.createDirectories(destination.getParent());
                        Files.copy(path, destination, StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    public static void deleteRecursively(Path path) {
        if (!Files.exists(path)) return;
        try (Stream<Path> stream = Files.walk(path)) {
            stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (IOException ignored) { }
            });
        } catch (IOException ignored) {
            // Best effort: staging leftovers are cleared again on next startup.
        }
    }
}
