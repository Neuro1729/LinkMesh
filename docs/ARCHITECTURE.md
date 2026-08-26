# Architecture

## Roles

Two programs, both in the same jar.

**Controller** holds no data. It tracks membership, decides where replicas live,
schedules map tasks, and collects reduce output. Restarting it loses no data,
because the data is on the nodes and their inventories get re-read from them.

**Node** is a storage node, a compute node, and possibly a reducer, all in one
process. That combination is what makes locality scheduling possible: the
controller can put a task on a machine that already has the bytes.

```
                        Controller
             membership, placement, scheduling
                     /      |      \
               Node A     Node B    Node C
              [P0 P3]    [P0 P1]   [P1 P3]     replicas, RF=2
                  |         |         |
                  +-- gossip mesh ----+
```

## Wire protocol

A readable header line, then an optional length-prefixed binary body:

```
VERB<TAB>key=value<TAB>key=value<TAB>_len=1048576<LF>
<1048576 raw bytes>
```

The header stays greppable in a packet dump; the body carries bulk data with no
encoding overhead. Connections are persistent and pooled, so a shuffle batch
costs one write instead of a TCP handshake.

## Storage

Each node keeps a durable local store:

```
linkmesh-data/<node-id>/
  partitions/<id>/     page files
  meta/<id>.meta       byte count, file count, SHA-256
  tmp/                 staging for in-flight transfers
```

Incoming partitions unpack into `tmp/`, get digest-checked, and only then move
into place. An interrupted transfer cannot leave a half-written partition that
later reads as complete.

Partition content is immutable, so a push whose digest matches what is already
stored is skipped. That also stops a re-replication from pulling the directory
out from under a task reading it.

## Placement

`PlacementPlanner` is a pure function of (placement, membership). It emits
`REPLICATE` and `DROP` moves that push the cluster toward exactly
`replicationFactor` copies of every partition, spread evenly. The controller
applies them; the planner does no IO.

Placement is refreshed from the nodes every couple of seconds, so an interrupted
transfer, a manual deletion, or a node rejoining with a partial store all
self-correct. A short grace window keeps replicas whose transfer is still in
flight from being erased by a report from a node that legitimately does not have
them yet. The scheduler only ever dispatches to confirmed replicas.

## Failure detection

Two signals, because each catches what the other misses:

| Signal | Latency | Catches |
|---|---|---|
| Control connection closes | milliseconds | Process exited or was killed |
| Gossip silence, ALIVE to SUSPECT to DEAD | seconds | Wedged, swapping, GC-paused, partitioned |

Every node keeps one connection to the controller that is never returned to the
pool. When the process dies the socket closes and the controller evicts it at
once. A hung node keeps that socket open while doing no work, and only the peers
trying to talk to it notice, which is what gossip is for.

SUSPECT exists because a slow node and a dead node look identical from outside.
The intermediate state gives a late heartbeat time to clear the suspicion before
re-replication starts.

## Scheduling

A partition lives on `replicationFactor` machines. The scheduler prefers an idle
machine that already holds it, so the common case moves no data. If every holder
is busy, an idle non-holder takes the task and pulls the partition first.

`--slots` controls how many tasks a node runs at once, and on a multi-core
machine it is the knob that matters. Each task builds its own thread pools and
tears them down, and one partition often lacks the files to keep a wide parser
pool busy, so throughput tracks tasks in flight rather than threads per task.
Measured on 8 cores: 1 slot to 4 slots cut the map stage from 12.4s to 3.9s,
while widening the parser pool from 2 to 8 threads changed almost nothing.

Reusing pools across tasks instead of rebuilding them per task would remove most
of that gap. Not done yet.

## Speculation

Once enough tasks have finished to establish a median, any attempt running past
`max(minSpeculativeMs, median * multiplier)` gets a second attempt elsewhere.
First to finish wins, the loser is cancelled.

No commit protocol is needed, and the reason is specific. The reducer stores
backlinks as `Map<String, Set<String>>`, so applying the edge `(target, source)`
twice is a no-op. Two attempts of the same partition converge on an identical
index however their output interleaves, and the loser's partial output needs no
fencing.

A reduce that summed, counted, or averaged would not have that property and
would need the losing attempt fenced off first. This is a property of the
workload, not a general result.

## Map pipeline, per task

```
partition directory
     |  ForkJoinPool, recursive work-stealing scan
     v
BoundedTaskQueue<Path>        backpressure, instrumented
     |  fixed platform thread pool
     v
parse page, emit edges
     |  per-reducer buffers, batched
     v
ShuffleWriter -> reducer
```

Three concurrency tools, each picked for its workload:

- **ForkJoinPool** for discovery. The directory tree is uneven and work stealing
  balances subtrees without manual partitioning.
- **Fixed platform pool** for parsing. Uniform independent tasks, so stealing
  would only add overhead, and CPU-bound work gains nothing from virtual threads.
- **Virtual threads** for connection handling, where many long-lived mostly-idle
  connections park cheaply.

`BoundedTaskQueue` is built on `ReentrantLock` and two `Condition`s rather than
`ArrayBlockingQueue`, so backpressure is explicit and instrumented.
`producerWaits` and `queueHighWater` let a run show the producer actually
blocked instead of asserting that backpressure exists.

The `await` calls sit in `while` loops, not `if`s: a signalled thread can lose
the race to another consumer, and spurious wakeups are permitted.

## Shuffle

Partitioning is `hash(targetUrl) mod reducerCount`, so every occurrence of a key
lands on one reducer. Ingest hashes the *source* URL instead; moving an edge from
its source partition to its target reducer is the whole job of the map stage.

Each reducer has its own buffer and lock. The critical section only appends and
swaps; the network send happens with no lock held, so reducers never contend and
a slow one does not stall parsing bound for the others.

## Reducer memory

Source URLs are interned. A page with 43 outbound links contributes its own URL
to 43 target sets, and each batch decode allocates a fresh String, so without
interning the store holds one String per edge rather than one per page. On the
Wikipedia corpus that was the difference between fitting in a 512 MB heap and
an OutOfMemoryError.

Budget roughly 100-200 bytes per unique edge after interning.

## Known limitations

- **The controller is a single point of failure for scheduling.** Data survives
  its loss; in-flight jobs do not.
- **Reducer state is in memory and not replicated.** Losing a reducer mid-job
  fails the job. Past roughly 10M edges per reducer it needs to spill to disk.
- **Reducer skew is real on web data.** Hash partitioning spreads keys evenly,
  not key sizes. Whichever reducer owns the hot keys finishes last. `max_fan_in`
  is reported so the skew is visible.
- **Thread pools are rebuilt per task**, which is why `--slots` matters so much.
- **A partition archive is buffered in memory during transfer.** Raise
  `--partitions` rather than partition size.
- **No authentication or TLS.** Trusted networks only.
