package linkmesh.proto;

/** Every verb the cluster speaks, in one place. */
public final class Verbs {
    private Verbs() {}

    // Generic replies
    public static final String OK = "OK";
    public static final String ERR = "ERR";

    // Membership: worker -> controller
    public static final String HELLO = "HELLO";              // node announces itself + inventory
    public static final String HEARTBEAT = "HEARTBEAT";      // liveness + load report
    public static final String GOSSIP = "GOSSIP";            // peer-observed membership digest
    public static final String NODE_STATUS = "NODE_STATUS";  // peer reports a suspicion

    // Cluster info: controller -> worker
    public static final String PEERS = "PEERS";              // current membership snapshot
    public static final String REDUCERS = "REDUCERS";        // reducer assignment for a job

    // Storage placement
    public static final String STORE_PUT = "STORE_PUT";      // push a partition archive to a node
    public static final String STORE_FETCH = "STORE_FETCH";  // pull a partition archive from a node
    public static final String STORE_DROP = "STORE_DROP";    // delete a local replica
    public static final String STORE_LIST = "STORE_LIST";    // inventory request
    public static final String REPLICATE = "REPLICATE";      // "send partition P to node N"
    public static final String PLACE = "PLACE";              // ingester asks where a new partition should go

    // Job execution
    public static final String SUBMIT = "SUBMIT";            // client -> controller
    public static final String RUN_TASK = "RUN_TASK";        // controller -> worker
    public static final String CANCEL_TASK = "CANCEL_TASK";  // controller -> worker (speculation loser)
    public static final String TASK_DONE = "TASK_DONE";
    public static final String TASK_FAILED = "TASK_FAILED";
    public static final String TASK_PROGRESS = "TASK_PROGRESS";

    // Shuffle and reduce
    public static final String MAP_BATCH = "MAP_BATCH";      // mapper -> reducer
    public static final String FINALIZE = "FINALIZE";        // controller -> reducer
    public static final String REDUCE_CHUNK = "REDUCE_CHUNK";
    public static final String REDUCE_DONE = "REDUCE_DONE";

    // Client tooling
    public static final String STATUS = "STATUS";
    public static final String CANCEL_JOB = "CANCEL_JOB";
}
