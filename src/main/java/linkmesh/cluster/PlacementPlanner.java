package linkmesh.cluster;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Works out the moves needed to get every partition onto exactly
 * replicationFactor live nodes, spread evenly.
 *
 * Pure function of (placement, membership): the controller applies the plan,
 * the planner never does IO.
 */
public final class PlacementPlanner {

    public enum Kind { REPLICATE, DROP }

    /** For REPLICATE, source is the node to copy from. For DROP, source is the node to delete on. */
    public record Move(Kind kind, String partition, String source, String target) {
        @Override
        public String toString() {
            return kind == Kind.REPLICATE
                    ? "REPLICATE " + partition + " " + source + " -> " + target
                    : "DROP " + partition + " on " + source;
        }
    }

    private final int replicationFactor;

    public PlacementPlanner(int replicationFactor) {
        if (replicationFactor < 1) throw new IllegalArgumentException("replicationFactor must be >= 1");
        this.replicationFactor = replicationFactor;
    }

    public int replicationFactor() { return replicationFactor; }

    public List<Move> plan(Placement placement, ClusterState cluster) {
        List<NodeInfo> alive = cluster.alive();
        List<Move> moves = new ArrayList<>();
        if (alive.isEmpty()) return moves;

        // Projected load, updated as moves are planned so one pass spreads evenly
        // instead of piling every under-replicated partition onto the same node.
        Map<String, Integer> projectedLoad = new HashMap<>();
        for (NodeInfo node : alive) projectedLoad.put(node.id, placement.loadOf(node.id));

        int effectiveFactor = Math.min(replicationFactor, alive.size());

        for (String partition : placement.sortedPartitions()) {
            List<String> holders = placement.aliveHolders(partition, cluster);

            if (holders.size() < effectiveFactor && !holders.isEmpty()) {
                String source = leastLoaded(holders, projectedLoad);
                int needed = effectiveFactor - holders.size();
                Set<String> exclude = new java.util.HashSet<>(holders);
                for (int i = 0; i < needed; i++) {
                    String target = pickTarget(alive, exclude, projectedLoad);
                    if (target == null) break;
                    moves.add(new Move(Kind.REPLICATE, partition, source, target));
                    exclude.add(target);
                    projectedLoad.merge(target, 1, Integer::sum);
                }
            } else if (holders.size() > effectiveFactor) {
                int excess = holders.size() - effectiveFactor;
                List<String> victims = new ArrayList<>(holders);
                victims.sort(Comparator.comparingInt((String id) -> projectedLoad.getOrDefault(id, 0)).reversed());
                for (int i = 0; i < excess; i++) {
                    String victim = victims.get(i);
                    moves.add(new Move(Kind.DROP, partition, victim, null));
                    projectedLoad.merge(victim, -1, Integer::sum);
                }
            }
        }
        return moves;
    }

    /** Initial spread of a freshly ingested partition across the cluster. */
    public List<String> chooseInitialHolders(Placement placement, ClusterState cluster, String partition) {
        List<NodeInfo> alive = cluster.alive();
        if (alive.isEmpty()) return List.of();
        Map<String, Integer> load = new HashMap<>();
        for (NodeInfo node : alive) load.put(node.id, placement.loadOf(node.id));

        List<String> chosen = new ArrayList<>();
        Set<String> exclude = new java.util.HashSet<>();
        int wanted = Math.min(replicationFactor, alive.size());
        for (int i = 0; i < wanted; i++) {
            String target = pickTarget(alive, exclude, load);
            if (target == null) break;
            chosen.add(target);
            exclude.add(target);
            load.merge(target, 1, Integer::sum);
        }
        return chosen;
    }

    private String pickTarget(List<NodeInfo> alive, Set<String> exclude, Map<String, Integer> load) {
        String best = null;
        int bestLoad = Integer.MAX_VALUE;
        for (NodeInfo node : alive) {
            if (exclude.contains(node.id)) continue;
            int nodeLoad = load.getOrDefault(node.id, 0);
            if (nodeLoad < bestLoad || (nodeLoad == bestLoad && best != null && node.id.compareTo(best) < 0)) {
                best = node.id;
                bestLoad = nodeLoad;
            }
        }
        return best;
    }

    private String leastLoaded(List<String> candidates, Map<String, Integer> load) {
        String best = candidates.get(0);
        for (String candidate : candidates) {
            if (load.getOrDefault(candidate, 0) < load.getOrDefault(best, 0)) best = candidate;
        }
        return best;
    }
}
