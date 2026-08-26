package linkmesh.cluster;

/**
 * What a machine is willing to be used for.
 *
 * Every node stores data and runs map tasks regardless. This only sets a
 * preference about the reduce side, which is the role that actually costs
 * something: a reducer holds its slice of the index in memory and takes inbound
 * shuffle from every mapper. Put it on the machines with RAM to spare.
 */
public enum NodeRole {
    /** Eligible to be a reducer, no preference either way. The default. */
    AUTO,
    /** Prefer this machine when choosing reducers. */
    REDUCER,
    /** Never choose this machine as a reducer unless there is no other option. */
    MAPPER;

    public static NodeRole parse(String value) {
        if (value == null || value.isBlank()) return AUTO;
        return switch (value.trim().toLowerCase()) {
            case "reducer" -> REDUCER;
            case "mapper", "map" -> MAPPER;
            case "auto", "any", "both" -> AUTO;
            default -> throw new IllegalArgumentException(
                    "unknown role: " + value + " (expected auto, reducer, or mapper)");
        };
    }
}
