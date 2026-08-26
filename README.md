# LinkMesh

[![CI](https://github.com/Neuro1729/LinkMesh/actions/workflows/ci.yml/badge.svg)](https://github.com/Neuro1729/LinkMesh/actions/workflows/ci.yml)

A distributed MapReduce engine in Java 21 that builds **backlink indexes**: for
every URL, the set of pages linking to it.

No Spring, no Kafka, no Hadoop, no Maven or Gradle, no third-party dependencies.
One 180 KB jar.

```bash
./scripts/build.sh
./scripts/demo.sh
```

## The problem

Input is grouped by source page. The answer is grouped by target page.

```
A links to B, C          B  <-  A
B links to C      ==>    C  <-  A, B, D
D links to C
```

Trivial for three pages. With millions you cannot hold it on one machine and flip
it, so the regrouping has to happen across a cluster. That regrouping is the
shuffle, and it is the whole job.

## Terms

| term | meaning |
|---|---|
| **edge** | one link, `(source, target)` |
| **key** | a target URL in the index, with its set of sources |
| **fan-in** | how many pages link to one key |
| **partition** | a chunk of the corpus, sharded by `hash(source)`. Unit of storage and of work |
| **map stage** | read pages, emit edges. Parallel across partitions |
| **shuffle** | send each edge to the node owning its target, via `hash(target) % reducers` |
| **reduce stage** | group arrivals into `Map<target, Set<source>>` |
| **controller** | one process: membership, placement, scheduling, output. Holds no data |
| **node** | stores partitions, runs map tasks, maybe acts as reducer. All three at once |
| **reducer** | a node holding part of the index in memory for a job. The role that costs RAM |
| **slots** | concurrent map tasks per node. The main throughput knob |
| **replication factor** | copies of each partition. Default 2 |

## Running a cluster

```bash
# one machine
java -jar linkmesh.jar controller --port 9000 --replication 2 --reducers 2

# every other machine, one flag
java -jar linkmesh.jar worker --controller 192.168.1.10:9000

# put reducers where the RAM is
java -Xmx8g -jar linkmesh.jar worker --controller HOST:9000 --role reducer
```

Node id, address, port and data directory are derived. A node reports what it
already holds on rejoin, so restarts are cheap.

Full guide including firewall rules: [docs/OPERATING.md](docs/OPERATING.md).

## Loading data

LinkMesh does not crawl. It indexes what you give it.

```bash
java -jar linkmesh.jar ingest --controller HOST:9000 --wikipedia dump.tar.gz --partitions 64
java -jar linkmesh.jar ingest --controller HOST:9000 --warc segment.warc.gz --partitions 64
java -jar linkmesh.jar ingest --controller HOST:9000 --edges web-BerkStan.txt.gz --partitions 32

java -jar linkmesh.jar submit --controller HOST:9000
java -jar linkmesh.jar status --controller HOST:9000
```

WARC rather than a folder of `.html` files, because a saved page has lost the URL
it came from and `href="/about"` cannot be resolved without it. The index would
come out wrong without complaining.

URLs are normalized: host lowercased, fragment dropped, default port dropped,
relative links resolved against `<base href>`, entities decoded. Skip that and
`example.com/x`, `example.com/x#top` and `example.com:443/x` become three keys.
Same-site links are dropped by default, since most links on a page are the site's
own navigation. Wikipedia is the exception and keeps them.

---

## Results

Two machines. **Laptop**: 8 cores, 5.7 GB. **CI runner**: 4 cores, 15 GB, and for
multi-machine tests each node gets its own runner joined over Tailscale.

Every configuration below produced a byte-identical index, checked by SHA-256.

Each job also reports `locality_pct`, the share of tasks that ran on a node
already holding the partition rather than fetching it. It is 100% on every run
here, which is the point of storing data on the nodes at all: with RF=2 and
partitions well spread, the scheduler almost never has to move bytes to find a
free machine.

### Datasets

| dataset | nodes | edges | keys | max fan-in |
|---|---|---|---|---|
| Simple English Wikipedia | 164,601 | 7,049,474 | 273,052 | 19,242 |
| web-BerkStan | 685,230 | 7,600,595 | 617,094 | 84,208 |
| soc-LiveJournal1 | 4,847,571 | 68,993,773 | — | — |

The SNAP graphs publish exact edge counts, so extraction is self-checking: it
produced exactly 7,600,595 and exactly 68,993,773.

### Wikipedia, one node, 4 slots

```
map stage 10.2 s   total 16.8 s   164,601 pages   7,049,474 links   273,052 keys
```

About 16,000 pages/sec. Output is 358 MB of TSV.

### Slots are the knob that matters

One node, 61,322 articles, varying `--slots`:

| slots | map stage | speedup |
|---|---|---|
| 1 | 12,378 ms | 1.00x |
| 2 | 6,372 ms | 1.94x |
| 4 | 3,618 ms | 3.42x |
| 8 | 3,621 ms | 3.42x |

Widening `--parserThreads` from 2 to 8 instead: 12,376 ms, no change.

A task builds its own thread pools, scans, parses, flushes, tears them down. Only
the parsing is parallel inside a task, and a partition holds ~11 files, so extra
threads idle. Overlapping whole tasks is what wins. Flat past 4 because 4 slots x
2 threads saturates 8 cores. `--slots` now defaults to cores/2.

### More machines: yes. More processes on one machine: no

Each node on its own runner, web-BerkStan, `--shuffleBatch 8192`:

| machines | slots each | tasks at once | map stage | vs 1x1 |
|---|---|---|---|---|
| 1 | 1 | 1 | 18,655 ms | 1.00x |
| 2 | 1 | 2 | 14,049 ms | 1.33x |
| 4 | 1 | 4 | 8,731 ms | 2.14x |
| 1 | 4 | 4 | 4,631 ms | 4.03x |
| 2 | 4 | 8 | 3,891 ms | 4.79x |
| 4 | 4 | 16 | 2,681 ms | **6.96x** |

**Fill a machine before adding another.** The two rows with 4 tasks in flight:
one machine at 4 slots is 4,631 ms, four machines at 1 slot is 8,731 ms. Same
concurrency, **1.89x apart**, because spreading the work pushes shuffle traffic
onto the network.

Splitting a *single* host into more node processes is strictly worse, with total
tasks held at 4:

| layout | laptop | CI runner |
|---|---|---|
| 1 node x 4 slots | 3,324 ms (1.00x) | 5,021 ms (1.00x) |
| 2 nodes x 2 slots | 4,288 ms (0.78x) | 5,716 ms (0.88x) |
| 4 nodes x 1 slot | 5,004 ms (0.66x) | 6,015 ms (0.83x) |

Four JVMs on one box do not create four machines of CPU. They add heaps, GC
threads, and a TCP hop where a method call used to be.

### The bug only a real network could show

First multi-machine attempt, `--shuffleBatch 512`:

| machines | map stage |
|---|---|
| 1 | 6,341 ms |
| 2 | **39,976 ms** |

Two machines six times slower than one. `ShuffleWriter` sends a batch and blocks
for the reply. On one machine the reducer is the same process, so 512 records per
batch was invisible. Across machines each batch became a network round trip:
~14,800 batches, half of them crossing a WireGuard tunnel between datacenters.

Changing only the batch size, at 2 machines:

| shuffle batch | map stage |
|---|---|
| 512 | 39,976 ms |
| 8192 | 3,891 ms |

**10.3x**, turning "6x slower" into "1.19x faster". Default is now 4096. The real
fix is to pipeline sends instead of blocking on each one. Not done yet.

### Speedup does not degrade with graph size

One dataset fed progressively more of itself, heap fixed at 10g, one reducer:

| edges | vs smallest | speedup at 4 slots |
|---|---|---|
| 7.4M | 1.0x | 1.29x |
| 17.2M | 2.3x | 1.48x |
| 34.5M | 4.7x | 1.42x |
| 69.0M | 9.3x | 1.33x |

Flat across 9.3x more data. Per-edge cost does rise 21% over that range as the
reducer's hash table outgrows cache, but it hits serial and parallel runs alike.

An earlier version of this README claimed scaling degraded with size, comparing
BerkStan at 3.24x against LiveJournal at 1.38x. That comparison also changed
partition count, heap and trial count, and at equal size the gap is still there:
7.4M edges of LiveJournal gives 1.29x where 7.6M of BerkStan gives 3.24x. It is a
difference between graphs, not sizes. LiveJournal has far more distinct nodes per
edge, so a much larger hash table.

### Skew

Fan-in on the Wikipedia run:

| backlinks | keys |
|---|---|
| 1 | 91,504 |
| 2-10 | 104,600 |
| 11-100 | 62,982 |
| 101-1,000 | 13,783 |
| 1,000+ | 183 |

`hash(target) % reducers` spreads keys evenly, not key *sizes*, and sizes span
five orders of magnitude. Whichever reducer owns `United_States` finishes last.
`max_fan_in` is reported so the skew is visible.

### Memory ceiling

69M edges index on one node with an 11 GB heap. Split across 4 nodes at 5 GB each
they **fail**: 2 reducers means ~34.5M edges each, which does not fit.

Roughly **35M edges per reducer per 5 GB**, about 150 bytes per edge. Most of that
is per-key overhead: each key holds a String, a map entry, and a `Set` that is
itself a hash map costing ~200 bytes even with one element.

| edges | total reducer heap |
|---|---|
| 2.6M | ~400 MB |
| 7.0M | ~1.2 GB |
| 69M | ~11 GB |

Source URLs are interned. A page with 43 links contributes its URL to 43 sets,
and each batch decode allocated a fresh String. Before interning, 2.6M edges did
not fit in 512 MB; after, they do.

### Failure and stragglers

```bash
./scripts/failure-demo.sh      # kills a node mid-job
./scripts/straggler-demo.sh    # slows one node down
```

Killing a node mid-job leaves the sorted output hash **unchanged**, and the job
now reports what recovery cost:

```
node_failures=1              tasks_rescheduled=1
failure_last_contact_ms=736  replications_issued=8
replication_recovery_ms=2060
```

One task redone, 8 partition copies issued, and full replication restored 2.1 s
after the loss. Killing a node on a real 4-machine cluster instead: 3 tasks
rescheduled and full replication restored in **3.0 s** across the network.

RF=2 means the data survives, and re-running the task re-sends the same edges,
which a `Set` absorbs.

`failure_last_contact_ms` is staleness of the last heartbeat when the node was
declared dead, not detection latency: on the connection-close path the
declaration is immediate. It matters for the other path, where a node goes quiet
with its socket still open and this climbs toward `--deadMs`. Death is detected in milliseconds because each node holds one dedicated
TCP connection to the controller, so a dead process shows as a closed socket
rather than a timeout. Gossip covers the other case, a node hung with its socket
still open.

Speculation: 4,332 ms without, 3,533 ms with. A straggler at 1,036 ms against a
73 ms median got a backup that finished in 55 ms, because that machine already
held a replica.

Running a task twice needs no commit protocol here, because applying an edge
twice to a `Set` is a no-op. A reduce that summed or counted would need the loser
fenced off first.

### Reproducing

```bash
gh workflow run benchmark.yml     -f dataset=web-BerkStan
gh workflow run scaling.yml       -f dataset=soc-LiveJournal1 -f heap=10g
gh workflow run multi-machine.yml -f workers=4 -f slots=4 -f shuffle_batch=8192
```

CI checks correctness on every push: 3-node and 1-node runs must agree, a killed
node must not change the result, speculation must not change the result, and a
hand-built WARC must normalize to an exact expected index.

---

## Commands

```
controller   run the controller
worker       run a node (storage + compute + reducer)
ingest       --wikipedia | --warc | --edges | --prepared | --out
gen          generate a synthetic corpus
submit       start a job
status       cluster and job state
cancel       cancel the running job
```

`java -jar linkmesh.jar help` lists every flag. Any flag can come from
`LINKMESH_<FLAG>` in the environment.

## How it works

```
                        Controller
             membership, placement, scheduling
                     /      |      \
               Node A     Node B    Node C
              [P0 P3]    [P0 P1]   [P1 P3]     replicas, RF=2
                  |         |         |
                  +-- gossip mesh ----+
```

One map task:

```
partition directory
     |  ForkJoinPool, work-stealing directory scan
     v
BoundedTaskQueue          backpressure, instrumented
     |  fixed thread pool
     v
parse page, emit edges -> batched shuffle to reducers
```

ForkJoin for the uneven directory tree, a fixed pool for uniform CPU-bound
parsing, virtual threads for the many idle connections.
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) has the detail.

```
src/main/java/linkmesh/
  Main.java     CLI
  proto/        framed protocol, pooled connections, archive format
  cluster/      membership, placement map, planner
  storage/      local replica store
  controller/   scheduling, speculation, job state
  worker/       map pipeline, bounded queue, shuffle, reducer, gossip
  ingest/       Wikipedia, WARC and edge-list readers, URL normalization
```

45 files, about 6,100 lines.

### The replica deleted out from under a running task

A 4-machine failure run turned up map tasks dying on
`NoSuchFileException: .../pages-000001.page`. The job still finished, because a
failed task is just rescheduled, but the cause was worth chasing.

A map task reads a partition **by path, not by open handle**. The scan enumerates
`Path` objects into a bounded queue and the parser opens them much later, so
there is a window as wide as the queue is deep in which the files it is about to
open can be unlinked.

Two things widen that window into a real bug. An on-demand task fetch stores a
replica the planner never asked for, and the controller only learns of it on the
next inventory sweep. Placement then reads RF+1 holders and schedules a DROP of
the surplus, picking the *most loaded* holder -- often the node that just fetched
it and is reading it right now.

Reproduce it, no failure injection needed beyond a node rejoining with its old
replicas still on disk:

```bash
./scripts/reader-drop-demo.sh
```

```
INFO  [control] dropped surplus replica of part-010 from node-1
WARN  [map] map failure: NoSuchFileException: .../part-010/bucket-5/sub-1/pages-000019.page
```

The fix is a reader count in the store, not a bigger lock. `runTask` pins the
partition for the length of the scan; `drop` refuses while the count is above
zero and the controller re-plans on the next tick; the rebalancer skips any
partition with an in-flight attempt. Both layers hold on their own -- with the
controller-side skip disabled, the node still refuses:

```
INFO [store] not dropping part-007, 1 task(s) still reading it
INFO [sched] part-007 complete on node-1 in 2041 ms
INFO [control] dropped surplus replica of part-007 from node-1
```

Refused while read, dropped 200 ms after the task finished. Three runs hit the
race before the fix, three were clean after, and it is now a CI step.

`publish` had the same hazard in milder form: it renamed the live directory aside
and deleted it immediately, with a comment claiming open handles kept readers
safe. True for a descriptor already open, not for a path not yet opened. The
retired copy now stays in `tmp/` until the next start.

## Limits

- Controller is a single point of failure for scheduling. Data survives it,
  running jobs do not.
- Reducer state is in memory and unreplicated. Losing a reducer fails the job.
- Reducer skew is not handled.
- Shuffle blocks on every batch, so batch size matters a lot across a network.
- Thread pools are rebuilt per task, which is why `--slots` matters so much.
- Partition archives are buffered in memory during transfer.
- A refused drop is retried on the next planning tick rather than queued, so a
  surplus replica can outlive the job that pinned it by a few seconds.
- No auth, no TLS.
- Link extraction is regex-based, not a real HTML parser.

## Requirements

Java 21+. Bash for the scripts.
