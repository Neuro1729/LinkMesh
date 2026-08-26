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
| **partition** | a chunk of corpus sharded by `hash(source)`. Unit of storage and of work |
| **shuffle** | send each edge to the node owning its target, via `hash(target) % reducers` |
| **reducer** | a node holding part of the index in memory. The role that costs RAM |
| **slots** | concurrent map tasks per node. The main throughput knob |
| **RF** | replication factor, copies of each partition. Default 2 |

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
already holds on rejoin, so restarts are cheap. Full guide including firewall
rules: [docs/OPERATING.md](docs/OPERATING.md).

## Loading data

LinkMesh does not crawl. It indexes what you give it.

```bash
java -jar linkmesh.jar ingest --controller HOST:9000 --wikipedia dump.tar.gz --partitions 64
java -jar linkmesh.jar ingest --controller HOST:9000 --warc segment.warc.gz --partitions 64
java -jar linkmesh.jar ingest --controller HOST:9000 --edges web-BerkStan.txt.gz --partitions 32

java -jar linkmesh.jar submit --controller HOST:9000
```

WARC rather than a folder of `.html` files, because a saved page has lost the URL
it came from and `href="/about"` cannot be resolved without it — the index would
come out wrong without complaining. URLs are normalized (host lowercased,
fragment and default port dropped, relatives resolved against `<base href>`,
entities decoded); skip that and `example.com/x`, `example.com/x#top` and
`example.com:443/x` become three keys.

---

# Results

**Laptop**: 8 cores, 5.7 GB. **CI runner**: 4 cores, 15 GB. For multi-machine
tests each node gets its own runner, joined over Tailscale — separate hosts with
their own cores and a real network between them, not processes on one box.

Every configuration below produced a byte-identical index, checked by SHA-256.

| dataset | nodes | edges | keys | max fan-in |
|---|---|---|---|---|
| Simple English Wikipedia | 164,601 | 7,049,474 | 273,052 | 19,242 |
| web-BerkStan | 685,230 | 7,600,595 | 617,094 | 84,208 |
| soc-LiveJournal1 | 4,847,571 | 68,993,773 | — | — |

The SNAP graphs publish exact edge counts, so extraction is self-checking: it
produced exactly 7,600,595 and exactly 68,993,773.

## What real machines buy

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

Splitting a *single* host into more node processes is strictly worse, total tasks
held at 4:

| layout | laptop | CI runner |
|---|---|---|
| 1 node x 4 slots | 3,324 ms (1.00x) | 5,021 ms (1.00x) |
| 2 nodes x 2 slots | 4,288 ms (0.78x) | 5,716 ms (0.88x) |
| 4 nodes x 1 slot | 5,004 ms (0.66x) | 6,015 ms (0.83x) |

Four JVMs on one box do not create four machines of CPU. They add heaps, GC
threads, and a TCP hop where a method call used to be.

## The bug only a real network could show

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

## What one box hides

Three things are only visible once the nodes are actually separate.

**Locality stops being free.** On a single machine `locality_pct` is 100% on
every run. On 4 machines with 16 slots:

```
tasks_local=30  tasks_fetched=2  locality_pct=93.8
```

Both holders of a partition are sometimes busy, so an idle machine fetches it
instead of waiting. That trade only exists when there are real machines to be
idle.

**Reduce dominates, not map.** On the same run, `map_stage_ms=3729` against
`reduce_stage_ms=10506` — reduce is **2.8x** the map stage, because output
streams back to the controller over the network. In-process, it barely showed.

**Recovery is slower than loopback suggests.** Killing a node on the real
4-machine cluster: 3 tasks rescheduled, full replication restored in **3.0 s**.
The same kill on loopback restores in 2.1 s.

## Killing a node

```bash
./scripts/failure-demo.sh
```

The sorted output hash is **unchanged**, and the job reports what recovery cost:

```
node_failures=1              tasks_rescheduled=1
failure_last_contact_ms=736  replications_issued=8
replication_recovery_ms=2060
```

RF=2 means the data survives, and re-running a task re-sends the same edges,
which a `Set` absorbs.

`failure_last_contact_ms` is heartbeat staleness at the moment of declaration,
not detection latency: each node holds one dedicated TCP connection to the
controller, so a dead process shows as a closed socket in milliseconds. It
matters for the other path, where a node goes quiet with its socket still open.

Getting that number honest took three tries. It first read 158 ms while 19
transfers were still in flight, because placement is only as fresh as the last
inventory sweep. And the kill was hitting the wrong process — `kill -9` targeted
a `timeout` wrapper, and SIGKILL is not forwarded to children, so the worker
survived to the 8 s `--deadMs` path instead of the fast one.

**Stragglers** (`./scripts/straggler-demo.sh`): 4,332 ms without speculation,
3,533 ms with. A straggler at 1,036 ms against a 73 ms median got a backup that
finished in 55 ms, because that machine already held a replica. Running a task
twice needs no commit protocol here, since applying an edge twice to a `Set` is a
no-op. A reduce that summed or counted would need the loser fenced off first.

## The replica deleted out from under a running task

The 4-machine failure run turned up map tasks dying on `NoSuchFileException`. The
job still finished — a failed task is just rescheduled — but the cause was worth
chasing.

A map task reads a partition **by path, not by open handle**: the scan enumerates
`Path` objects into a bounded queue and the parser opens them much later, so the
files it is about to open can be unlinked in between. Measured, that window ran
to six seconds.

An on-demand task fetch stores a replica the planner never asked for, and the
controller only learns of it on the next inventory sweep. Placement then reads
RF+1 holders and schedules a DROP of the surplus, picking the *most loaded*
holder — often the node that just fetched it and is reading it right now.

```bash
./scripts/reader-drop-demo.sh
```

```
INFO  [control] dropped surplus replica of part-010 from node-1
WARN  [map] map failure: NoSuchFileException: .../part-010/bucket-5/sub-1/pages-000019.page
```

The fix is a reader count in the store, not a bigger lock. `runTask` pins the
partition for the scan; `drop` refuses while the count is above zero and the
controller re-plans next tick; the rebalancer skips partitions with an in-flight
attempt. Both layers hold alone — with the controller-side skip disabled, the
node still refuses:

```
INFO [store] not dropping part-007, 1 task(s) still reading it
INFO [sched] part-007 complete on node-1 in 2041 ms
INFO [control] dropped surplus replica of part-007 from node-1
```

Refused while read, dropped 200 ms after the task finished. Three runs hit the
race before the fix, three were clean after, and it is now a CI step.

`publish` had the same hazard in milder form: it renamed the live directory aside
and deleted it immediately, with a comment claiming open handles kept readers
safe — true for a descriptor already open, not for a path not yet opened. The
retired copy now stays in `tmp/` until the next start.

## On one machine

Wikipedia, one node, 4 slots: **10.2 s** map stage, 16.8 s total, 164,601 pages,
7,049,474 links. About 16,000 pages/sec, 358 MB of TSV out.

Varying `--slots` on 61,322 articles: 12,378 ms at 1, 6,372 at 2 (1.94x), 3,618
at 4 (3.42x), 3,621 at 8 (3.42x). Widening `--parserThreads` from 2 to 8 instead
changes nothing (12,376 ms). Only the parsing is parallel inside a task and a
partition holds ~11 files, so extra threads idle — overlapping whole tasks is
what wins. Flat past 4 because 4 slots x 2 threads saturates 8 cores. `--slots`
now defaults to cores/2.

**Speedup holds as the graph grows.** Heap fixed at 10g: 1.29x at 7.4M edges,
1.48x at 17.2M, 1.42x at 34.5M, 1.33x at 69.0M — flat across 9.3x more data. An
earlier version of this README read that as scaling degrading with size, but at
equal size the gap is between *graphs*, not sizes: 7.4M edges of LiveJournal
gives 1.29x where 7.6M of BerkStan gives 3.24x, because LiveJournal has far more
distinct nodes per edge and so a much larger hash table.

**Skew is the reduce-side ceiling.** Fan-in on the Wikipedia run runs from 91,504
keys with a single backlink to 183 keys with over a thousand. `hash(target) %
reducers` spreads keys evenly, not key *sizes*, and sizes span five orders of
magnitude, so whichever reducer owns `United_States` finishes last. `max_fan_in`
is reported to keep that visible.

**Memory ceiling**: roughly **35M edges per reducer per 5 GB**, about 150 bytes
per edge. 69M edges index on one node with an 11 GB heap; split across 4 nodes at
5 GB each they *fail*, since 2 reducers means ~34.5M edges each. Most of the cost
is per-key overhead — a String, a map entry, and a `Set` that is itself a hash
map costing ~200 bytes even with one element. Source URLs are interned: a page
with 43 links contributes its URL to 43 sets, and before interning 2.6M edges did
not fit in 512 MB.

## Reproducing

```bash
gh workflow run benchmark.yml     -f dataset=web-BerkStan
gh workflow run scaling.yml       -f dataset=soc-LiveJournal1 -f heap=10g
gh workflow run multi-machine.yml -f workers=4 -f slots=4 -f shuffle_batch=8192
```

CI checks correctness on every push: 3-node and 1-node runs must agree, a killed
node must not change the result, speculation must not change the result, a
replica must not be dropped under a running task, and a hand-built WARC must
normalize to an exact expected index.

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

45 files, about 6,100 lines across `proto/` (framed protocol, pooled
connections, archive format), `cluster/`, `storage/`, `controller/`, `worker/`
and `ingest/`. [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) has the detail.

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
