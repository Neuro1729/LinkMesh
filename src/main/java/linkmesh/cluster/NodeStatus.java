package linkmesh.cluster;

/**
 * Ordered deliberately: a merge of two conflicting reports at the same
 * heartbeat takes the more pessimistic one, so suspicion propagates but
 * cannot be silently downgraded without a fresher heartbeat.
 */
public enum NodeStatus {
    ALIVE,
    SUSPECT,
    DEAD;

    public boolean isUsable() { return this == ALIVE; }
}
