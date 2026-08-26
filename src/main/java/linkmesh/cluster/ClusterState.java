package linkmesh.cluster;

import linkmesh.common.Log;
import linkmesh.proto.Endpoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Membership as the controller sees it.
 *
 * Fed by two signals: the control connection dropping (immediate, catches
 * killed processes) and gossip-observed silence (slower, catches nodes that are
 * hung but still connected). Neither alone covers both cases.
 */
public final class ClusterState {
    private static final Log log = Log.of("cluster");

    public interface MembershipListener {
        default void onJoin(NodeInfo node) {}

        default void onStatusChange(NodeInfo node, NodeStatus previous, NodeStatus current) {}
    }

    private final Map<String, NodeInfo> nodes = new ConcurrentHashMap<>();
    private final List<MembershipListener> listeners = new CopyOnWriteArrayList<>();
    private final long suspectMillis;
    private final long deadMillis;

    public ClusterState(long suspectMillis, long deadMillis) {
        this.suspectMillis = suspectMillis;
        this.deadMillis = deadMillis;
    }

    public void addListener(MembershipListener listener) { listeners.add(listener); }

    /** Idempotent: a restarting node reclaims its id and is revived if it was dead. */
    public NodeInfo register(String id, Endpoint endpoint) {
        NodeInfo existing = nodes.get(id);
        if (existing != null && existing.endpoint.equals(endpoint)) {
            existing.touch();
            transition(existing, NodeStatus.ALIVE);
            return existing;
        }
        NodeInfo node = new NodeInfo(id, endpoint);
        nodes.put(id, node);
        log.info("node joined: %s", node);
        for (MembershipListener listener : listeners) listener.onJoin(node);
        return node;
    }

    public NodeInfo get(String id) { return nodes.get(id); }

    public Collection<NodeInfo> all() { return nodes.values(); }

    public List<NodeInfo> alive() {
        List<NodeInfo> result = new ArrayList<>();
        for (NodeInfo node : nodes.values()) if (node.isAlive()) result.add(node);
        result.sort(Comparator.comparing(n -> n.id));
        return result;
    }

    public int aliveCount() {
        int count = 0;
        for (NodeInfo node : nodes.values()) if (node.isAlive()) count++;
        return count;
    }

    public void heartbeat(String id, long counter) {
        NodeInfo node = nodes.get(id);
        if (node == null) return;
        if (counter > node.heartbeat()) node.setHeartbeat(counter);
        node.touch();
        transition(node, NodeStatus.ALIVE);
    }

    /** Applied when a peer reports a suspicion via gossip. Never revives a dead node. */
    public void applyPeerReport(String reporterId, String targetId, NodeStatus reported) {
        NodeInfo node = nodes.get(targetId);
        if (node == null || node.status() == NodeStatus.DEAD) return;
        if (reported.ordinal() > node.status().ordinal()) {
            log.info("gossip from %s: %s -> %s", reporterId, targetId, reported);
            transition(node, reported);
        }
    }

    /** Immediate eviction on control-connection loss. */
    public void markDead(String id, String reason) {
        NodeInfo node = nodes.get(id);
        if (node == null || node.status() == NodeStatus.DEAD) return;
        log.warn("node %s declared DEAD (%s)", id, reason);
        node.resetTasks();
        transition(node, NodeStatus.DEAD);
    }

    /** Periodic timeout sweep. Returns nodes that newly became DEAD. */
    public List<NodeInfo> sweep() {
        List<NodeInfo> newlyDead = new ArrayList<>();
        for (NodeInfo node : nodes.values()) {
            if (node.status() == NodeStatus.DEAD) continue;
            long silent = node.silentMillis();
            NodeStatus target = silent >= deadMillis ? NodeStatus.DEAD
                    : silent >= suspectMillis ? NodeStatus.SUSPECT
                    : NodeStatus.ALIVE;
            if (target != node.status()) {
                if (target == NodeStatus.DEAD) {
                    log.warn("node %s silent for %d ms -> DEAD", node.id, silent);
                    node.resetTasks();
                    newlyDead.add(node);
                } else if (target == NodeStatus.SUSPECT) {
                    log.info("node %s silent for %d ms -> SUSPECT", node.id, silent);
                }
                transition(node, target);
            }
        }
        return newlyDead;
    }

    private void transition(NodeInfo node, NodeStatus target) {
        NodeStatus previous = node.status();
        if (previous == target) return;
        if (previous == NodeStatus.DEAD && target != NodeStatus.ALIVE) return;
        node.setStatus(target);
        for (MembershipListener listener : listeners) listener.onStatusChange(node, previous, target);
    }

    /** Compact digest broadcast to peers so they can seed their own gossip view. */
    public String encodeMembership() {
        StringBuilder sb = new StringBuilder();
        for (NodeInfo node : nodes.values()) {
            if (!sb.isEmpty()) sb.append(';');
            sb.append(node.id).append(',').append(node.endpoint.host()).append(',')
              .append(node.endpoint.port()).append(',').append(node.heartbeat()).append(',')
              .append(node.status().name());
        }
        return sb.toString();
    }
}
