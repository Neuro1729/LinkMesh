package linkmesh.cluster;

import linkmesh.proto.Endpoint;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** One cluster member: identity, address, liveness, inventory, and current load. */
public final class NodeInfo {
    public final String id;
    public final Endpoint endpoint;

    private volatile NodeStatus status = NodeStatus.ALIVE;
    private volatile NodeRole role = NodeRole.AUTO;
    private volatile long heartbeat;
    private volatile long lastSeenNanos = System.nanoTime();
    private volatile int mapSlots = 1;
    private volatile long storedBytes;

    private final AtomicInteger runningTasks = new AtomicInteger();
    private final Set<String> partitions = ConcurrentHashMap.newKeySet();

    public NodeInfo(String id, Endpoint endpoint) {
        this.id = id;
        this.endpoint = endpoint;
    }

    public NodeStatus status() { return status; }

    public NodeRole role() { return role; }

    public void setRole(NodeRole role) { this.role = role == null ? NodeRole.AUTO : role; }

    public void setStatus(NodeStatus status) { this.status = status; }

    public boolean isAlive() { return status == NodeStatus.ALIVE; }

    public long heartbeat() { return heartbeat; }

    public void setHeartbeat(long heartbeat) { this.heartbeat = heartbeat; }

    public long lastSeenNanos() { return lastSeenNanos; }

    public void touch() { this.lastSeenNanos = System.nanoTime(); }

    public long silentMillis() {
        return (System.nanoTime() - lastSeenNanos) / 1_000_000L;
    }

    public int mapSlots() { return mapSlots; }

    public void setMapSlots(int mapSlots) { this.mapSlots = Math.max(1, mapSlots); }

    public long storedBytes() { return storedBytes; }

    public void setStoredBytes(long storedBytes) { this.storedBytes = storedBytes; }

    public int runningTasks() { return runningTasks.get(); }

    public void taskStarted() { runningTasks.incrementAndGet(); }

    public void taskFinished() { runningTasks.updateAndGet(v -> Math.max(0, v - 1)); }

    public void resetTasks() { runningTasks.set(0); }

    public boolean hasFreeSlot() { return isAlive() && runningTasks.get() < mapSlots; }

    public Set<String> partitions() { return partitions; }

    public void replaceInventory(Set<String> ids) {
        partitions.retainAll(ids);
        partitions.addAll(ids);
    }

    @Override
    public String toString() {
        return id + "@" + endpoint + "[" + status + " " + role + " tasks=" + runningTasks.get()
                + "/" + mapSlots + " parts=" + partitions.size() + "]";
    }
}
