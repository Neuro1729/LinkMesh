# LinkMesh

A distributed MapReduce engine in Java 21 that builds backlink indexes: for every
URL, the set of pages linking to it.

No Spring, no Kafka, no Hadoop, no Maven or Gradle, and no third-party
dependencies at all. The cluster ships as one 180 KB jar.

```bash
./scripts/build.sh
./scripts/demo.sh
```

## What it does

- Shards a corpus into partitions and distributes them across machines
- Keeps every partition on N machines (default 2) and re-replicates when one dies
- Schedules map tasks onto machines that already hold the data
- Starts a backup attempt when a task runs long, takes whichever finishes first
- Reads Wikipedia dumps and Common Crawl WARC files
- Adding a machine takes one flag

## Cluster setup

Controller (one machine):

```bash
java -jar linkmesh.jar controller --port 9000 --replication 2 --reducers 2
```

Every other machine:

```bash
java -jar linkmesh.jar worker --controller 192.168.1.10:9000
```

Node id, LAN address, listen port and data directory are all derived. A node
reports whatever it already has on disk when it joins, so restarts are cheap.

Reducers hold their slice of the index in memory and absorb shuffle traffic from
every mapper, so put them on the machines with RAM:

```bash
java -Xmx8g -jar linkmesh.jar worker --controller HOST:9000 --role reducer
java -jar linkmesh.jar worker --controller HOST:9000 --role mapper
```

Full setup guide including firewall rules: [docs/OPERATING.md](docs/OPERATING.md).

## Results on real data

Measured on one laptop: 8 cores, 5.7 GB RAM, Windows 11, OpenJDK 21. Everything
below is specific to that machine.

### Dataset

Simple English Wikipedia, from the [Wikimedia Enterprise HTML
dump](https://dumps.wikimedia.org/other/enterprise_html/) (2025-03-20). Real
article HTML, real link structure.

| | |
|---|---|
| Articles indexed | 164,601 |
| Links extracted | 7,049,474 |
| Corpus on disk | 389 MB across 64 partitions |
| Extraction | 3 m 31 s from a 1.1 GB compressed dump |
| Load onto cluster | 30 s (774 MB including replicas) |

Wikipedia links are same-site, so this run keeps internal links. The default for
WARC input is the opposite, since on the open web most links are site navigation.

### Full corpus

```
METRIC map_stage_ms=10228
METRIC total_job_ms=16816
METRIC pages=164601
METRIC links_emitted=7049474
METRIC backlink_keys=273052
METRIC max_fan_in=19242
```

16.8 s end to end on a single node with `--slots 4`. That is about 16,000
pages/sec and 690,000 links/sec through the map stage. Output is 358 MB of TSV.

Most-referenced articles:

```
19242  United_States
17400  Daylight_saving_time
16278  Wayback_Machine
15270  Time_zone
13728  France
```

Backlinks to one article:

```
174 pages reference Albert_Einstein:
  1921, 1934, Aage_Niels_Bohr, Aarau, Age_of_the_universe,
  Albert_Abraham_Michelson, Albert_Einstein_Square, Alcubierre_drive, ...
```

Fan-in follows the power law you would expect:

| backlinks | keys |
|---|---|
| 1 | 91,504 |
| 2-10 | 104,600 |
| 11-100 | 62,982 |
| 101-1,000 | 13,783 |
| 1,000+ | 183 |

That last row is why reduce is the slow stage. Hash partitioning spreads keys
evenly but not key sizes, so whichever reducer owns `United_States` finishes
last.

### What actually makes it faster

61,322 articles / 2,599,585 links, median of 3 runs, 2 parser threads per task.

One node, varying `--slots` (concurrent map tasks):

| slots | map stage | speedup |
|---|---|---|
| 1 | 12,378 ms | 1.00x |
| 2 | 6,372 ms | 1.94x |
| 4 | 3,618 ms | 3.42x |
| 8 | 3,621 ms | 3.42x |

Near-linear to 4 slots, then flat: 4 slots x 2 parser threads is 8 threads on 8
cores. Widening the parser pool instead barely moves anything (1 node, 8 parser
threads, 1 slot was 12,376 ms, essentially the same as 2 threads). Throughput
tracks how many partitions are in flight, not how many threads each one gets,
because every task builds and tears down its own thread pools and one partition
rarely has enough files to keep a wide pool busy.

`--slots` now defaults to cores/2 for that reason.

### What distribution costs

Same total work in flight (4 concurrent tasks), split across more node processes
on the same machine:

| layout | map stage | vs one node |
|---|---|---|
| 1 node x 4 slots | 3,324 ms | 1.00x |
| 2 nodes x 2 slots | 4,288 ms | 0.78x |
| 4 nodes x 1 slot | 5,004 ms | 0.66x |

Splitting one machine into more node processes makes it **slower**, by a third at
4 nodes. Same story on the full corpus: 1 node x 4 slots ran it in 10.2 s of map
stage, 4 nodes x 1 slot took 15.3 s.

This is the honest result and it is worth stating plainly. Distributing does not
create CPU. On one box the extra JVMs compete for the same cores and pay
cross-process shuffle instead of in-process handoff. What distribution buys is
fault tolerance and the ability to use machines you would otherwise not have. To
measure a real speedup from adding nodes you need nodes with their own cores,
which is not something a single laptop can show.

Every configuration above produced byte-identical output (same SHA-256 over
193,326 keys).

Reproduce:

```bash
java -jar build/linkmesh.jar ingest --wikipedia DUMP.tar.gz --out build/wiki-corpus --partitions 64
CORPUS=build/wiki-corpus MODE=slots ./scripts/benchmark.sh
CORPUS=build/wiki-corpus MODE=fixed ./scripts/benchmark.sh
```

### Fault tolerance

```bash
./scripts/failure-demo.sh
```

Kills a node mid-job. The job finishes and the sorted output hash is unchanged,
because every partition had a second replica.

```
WARN  [cluster] node node-3 declared DEAD (control connection closed)
WARN  [sched]   part-004 will be rescheduled, lost with node node-3
INFO  [control] replicating part-001 from node-1 to node-2
```

Death is detected in milliseconds. Each node holds one control connection to the
controller that is never pooled, so a dead process shows up as a closed socket
rather than a timeout. Gossip between nodes covers the other case, a node that is
hung but still connected.

### Stragglers

```bash
./scripts/straggler-demo.sh
```

One node is slowed artificially:

```
map stage without speculation : 4332 ms
map stage with speculation    : 3533 ms

INFO [sched] straggler: part-009 on node-3 at 1036 ms vs median 73 ms,
             starting backup on node-1
INFO [sched] backup attempt won for part-009, cancelling the original
```

The backup finished in 55 ms because node-1 already had a replica.

Running a partition twice is safe here without a commit protocol: the reducer
stores backlinks in a `Set`, so applying the same edge twice does nothing and
both attempts converge on the same index. A reduce that summed or counted would
need the loser fenced off first.

## Loading data

Wikipedia:

```bash
java -jar linkmesh.jar ingest --controller HOST:9000 \
  --wikipedia simplewiki-NS0-ENTERPRISE-HTML.json.tar.gz --partitions 64
```

Common Crawl, or your own `wget --warc-file` crawl:

```bash
java -jar linkmesh.jar ingest --controller HOST:9000 --warc segment.warc.gz --partitions 64
```

WARC is required rather than a folder of `.html` files because each record
carries `WARC-Target-URI`. Without knowing where a page was fetched from, a
relative `href="/about"` cannot be resolved and the index comes out silently
wrong.

The ingester resolves relative links, honours `<base href>`, strips fragments,
lowercases hosts, drops default ports and decodes entities. Skip any of that and
`Example.com/x`, `example.com/x`, `example.com:80/x` and `example.com/x#top`
become four separate keys.

Then run a job:

```bash
java -jar linkmesh.jar submit --controller HOST:9000
java -jar linkmesh.jar status --controller HOST:9000
```

## Sizing

Reducer state lives in memory. Source URLs are interned, since a page with 43
outbound links otherwise contributes 43 copies of its own URL. Without interning
2.6M edges did not fit in a 512 MB heap; with it they do.

| articles | edges | total reducer heap |
|---|---|---|
| 60,000 | 2.6M | ~400 MB |
| 165,000 | 7.0M | ~1.2 GB |
| 500,000 | 21M | ~3.5 GB |

Divide by reducer count for per-machine heap. Past roughly 10M edges per reducer
you want disk-spilling reducers, which this does not do yet.

## Commands

```
controller   run the controller
worker       run a node (storage + compute + reducer)
ingest       load a corpus: --wikipedia, --warc, --prepared, or --out to stage locally
gen          generate a synthetic corpus
submit       start a job
status       cluster and job state
cancel       cancel the running job
```

`java -jar linkmesh.jar help` lists every flag. Any flag can also come from
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

Per node, the map stage is:

```
partition directory
     |  ForkJoinPool, work-stealing directory scan
     v
BoundedTaskQueue          backpressure, instrumented
     |  fixed thread pool
     v
parse page, emit edges -> batched shuffle to reducers
```

Three concurrency tools, picked per workload: ForkJoin for the uneven directory
tree, a fixed platform pool for uniform CPU-bound parsing, virtual threads for
the many mostly-idle connections.

[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) has the details.

```
src/main/java/linkmesh/
  Main.java        CLI
  proto/           framed protocol, pooled connections, archive format
  cluster/         membership, placement map, planner
  storage/         local replica store
  controller/      scheduling, speculation, job state
  worker/          map pipeline, bounded queue, shuffle, reducer, gossip
  ingest/          Wikipedia and WARC readers, link extraction, URL normalization
```

44 files, ~5,700 lines.

## Limits

- The controller is a single point of failure for scheduling. Data survives it,
  running jobs do not.
- Reducer state is in memory and unreplicated. Losing a reducer mid-job fails
  the job.
- Reducer skew is not addressed. One reducer gets the hot keys and finishes last.
- Partition archives are buffered in memory during transfer. Prefer more
  partitions over bigger ones.
- No auth, no TLS. Trusted networks only.
- Link extraction is regex-based rather than a real HTML parser.

## Requirements

Java 21+. Bash for the helper scripts.
