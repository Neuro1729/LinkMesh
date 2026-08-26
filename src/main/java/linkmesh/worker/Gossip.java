package linkmesh.worker;

import linkmesh.cluster.NodeStatus;
import linkmesh.common.Log;
import linkmesh.proto.ConnectionPool;
import linkmesh.proto.Endpoint;
import linkmesh.proto.Message;
import linkmesh.proto.Verbs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Peer-to-peer failure detection between nodes.
 *
 * Each node bumps a heartbeat counter and pushes its membership view to two
 * random peers per round. Merges take the higher heartbeat, so liveness spreads
 * without anyone probing everyone.
 *
 * The controller already detects a dead process from its dropped control
 * connection. Gossip covers the other case: a node that is wedged or GC-paused
 * keeps the socket open while doing no work, and only its peers notice.
 *
 * SUSPECT sits between ALIVE and DEAD because a slow node and a dead node look
 * identical from outside. The extra state gives a late heartbeat a chance to
 * arrive before re-replication kicks off.
 */
public final class Gossip implements AutoCloseable {
    private static final Log log = Log.of("gossip");

    private static final class Peer {
        final String id;
        final Endpoint endpoint;
        volatile long heartbeat;
        volatile long lastSeenNanos = System.nanoTime();
        volatile NodeStatus status = NodeStatus.ALIVE;

        Peer(String id, Endpoint endpoint, long heartbeat) {
            this.id = id;
            this.endpoint = endpoint;
            this.heartbeat = heartbeat;
        }
    }

    private final String selfId;
    private final Endpoint selfEndpoint;
    private final Endpoint controller;
    private final ConnectionPool pool;
    private final long suspectMillis;
    private final long deadMillis;
    private final int fanout;

    private final Map<String, Peer> peers = new ConcurrentHashMap<>();
    private final AtomicLong heartbeat = new AtomicLong();
    private final Random random = new Random();
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(2, runnable -> {
                Thread thread = new Thread(runnable, "gossip");
                thread.setDaemon(true);
                return thread;
            });

    public Gossip(String selfId, Endpoint selfEndpoint, Endpoint controller, ConnectionPool pool,
                  long suspectMillis, long deadMillis, int fanout) {
        this.selfId = selfId;
        this.selfEndpoint = selfEndpoint;
        this.controller = controller;
        this.pool = pool;
        this.suspectMillis = suspectMillis;
        this.deadMillis = deadMillis;
        this.fanout = Math.max(1, fanout);
    }

    public void start(long intervalMillis) {
        scheduler.scheduleAtFixedRate(this::round, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::detect, intervalMillis, Math.max(250, intervalMillis / 2), TimeUnit.MILLISECONDS);
    }

    /** Seeds the peer table from the controller membership snapshot. */
    public void seed(String encoded) {
        if (encoded == null || encoded.isBlank()) return;
        for (String item : encoded.split(";")) {
            if (item.isBlank()) continue;
            String[] parts = item.split(",", -1);
            if (parts.length < 3 || parts[0].equals(selfId)) continue;
            try {
                Endpoint endpoint = new Endpoint(parts[1], Integer.parseInt(parts[2]));
                peers.computeIfAbsent(parts[0], id -> new Peer(id, endpoint, 0));
            } catch (RuntimeException e) {
                log.debug("ignoring malformed peer entry: %s", item);
            }
        }
    }

    private void round() {
        try {
            long counter = heartbeat.incrementAndGet();
            List<Peer> candidates = new ArrayList<>();
            for (Peer peer : peers.values()) {
                if (peer.status != NodeStatus.DEAD) candidates.add(peer);
            }
            if (candidates.isEmpty()) return;

            Collections.shuffle(candidates, random);
            String digest = encodeView(counter);
            int targets = Math.min(fanout, candidates.size());
            for (int i = 0; i < targets; i++) {
                Peer peer = candidates.get(i);
                try {
                    pool.request(peer.endpoint, Message.of(Verbs.GOSSIP, "from", selfId).withBody(digest));
                } catch (IOException e) {
                    // Silence here is data, not an error. The detector below decides
                    // when accumulated silence becomes a suspicion.
                    log.debug("gossip to %s failed: %s", peer.id, e.getMessage());
                }
            }
        } catch (RuntimeException e) {
            log.warn("gossip round failed: %s", e.getMessage());
        }
    }

    private String encodeView(long selfHeartbeat) {
        StringBuilder sb = new StringBuilder();
        sb.append(selfId).append(',').append(selfEndpoint.host()).append(',')
          .append(selfEndpoint.port()).append(',').append(selfHeartbeat).append(',')
          .append(NodeStatus.ALIVE.name());
        for (Peer peer : peers.values()) {
            sb.append(';').append(peer.id).append(',').append(peer.endpoint.host()).append(',')
              .append(peer.endpoint.port()).append(',').append(peer.heartbeat).append(',')
              .append(peer.status.name());
        }
        return sb.toString();
    }

    /** Merges a digest received from a peer. Higher heartbeat always wins. */
    public void merge(String senderId, String digest) {
        long now = System.nanoTime();
        for (String item : digest.split(";")) {
            if (item.isBlank()) continue;
            String[] parts = item.split(",", -1);
            if (parts.length < 5) continue;
            String id = parts[0];
            if (id.equals(selfId)) continue;
            try {
                Endpoint endpoint = new Endpoint(parts[1], Integer.parseInt(parts[2]));
                long incoming = Long.parseLong(parts[3]);
                NodeStatus reported = NodeStatus.valueOf(parts[4]);
                peers.compute(id, (key, existing) -> {
                    if (existing == null) {
                        Peer created = new Peer(id, endpoint, incoming);
                        created.status = reported == NodeStatus.DEAD ? NodeStatus.DEAD : NodeStatus.ALIVE;
                        return created;
                    }
                    if (incoming > existing.heartbeat) {
                        existing.heartbeat = incoming;
                        existing.lastSeenNanos = now;
                        existing.status = NodeStatus.ALIVE;
                    } else if (incoming == existing.heartbeat && reported.ordinal() > existing.status.ordinal()) {
                        existing.status = reported;
                    }
                    return existing;
                });
            } catch (RuntimeException e) {
                log.debug("ignoring malformed digest entry: %s", item);
            }
        }
        Peer sender = peers.get(senderId);
        if (sender != null) sender.lastSeenNanos = now;
    }

    private void detect() {
        for (Peer peer : peers.values()) {
            long silentMillis = (System.nanoTime() - peer.lastSeenNanos) / 1_000_000L;
            NodeStatus target = silentMillis >= deadMillis ? NodeStatus.DEAD
                    : silentMillis >= suspectMillis ? NodeStatus.SUSPECT
                    : NodeStatus.ALIVE;
            if (target == peer.status) continue;
            peer.status = target;
            log.info("peer %s -> %s (silent %d ms)", peer.id, target, silentMillis);
            report(peer.id, target);
        }
    }

    private void report(String nodeId, NodeStatus status) {
        try {
            pool.request(controller, Message.of(Verbs.NODE_STATUS,
                    "from", selfId, "node", nodeId, "status", status.name()));
        } catch (IOException e) {
            log.debug("could not report %s status to controller: %s", nodeId, e.getMessage());
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
