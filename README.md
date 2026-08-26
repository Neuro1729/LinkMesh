# LinkMesh

[![CI](https://github.com/Neuro1729/LinkMesh/actions/workflows/ci.yml/badge.svg)](https://github.com/Neuro1729/LinkMesh/actions/workflows/ci.yml)

A distributed MapReduce engine in Java 21 that builds **backlink indexes**.

No Spring, no Kafka, no Hadoop, no Maven or Gradle, no third-party dependencies
at all. The whole cluster ships as one 180 KB jar.

## The problem it solves

You have a pile of web pages. Each page links *out* to other pages. You want the
opposite: for any page, who links *in* to it.

Say you have four pages:

```
A links to B, C
B links to C
D links to C
```

Turn that around and you get the backlink index:

```
B  <-  A
C  <-  A, B, D
```

Easy for four pages. The problem is that the input is grouped by *source* and the
answer is grouped by *target*, so once you have millions of pages you cannot hold
the whole thing on one machine and flip it. Doing that regrouping across many
machines is what this project is.

A real answer from a real run over 164,601 Wikipedia articles:

```
174 pages link to "Albert Einstein":
  1921, 1934, Aage_Niels_Bohr, Aarau, Age_of_the_universe,
  Albert_Abraham_Michelson, Albert_Einstein_Square, Alcubierre_drive, ...
```

## Quick start

```bash
./scripts/build.sh
./scripts/demo.sh
```

That builds the jar, generates a corpus, starts a 3-machine cluster on your
laptop, distributes the data, runs a job, and shows where everything landed.

---

## Terms

Worth reading once. The rest of the README uses these words precisely.

### The data

**Page** — one web page: a URL, plus the links going out of it.

**Edge** — one link, written `(source, target)`. Page A linking to page B is the
edge `(A, B)`.

**Key** — a target URL in the finished index. Each key has a set of sources
pointing at it.

**Fan-in** — how many pages link to one key. `United_States` had a fan-in of
19,242 in the Wikipedia run. This number matters more than it looks; see
[Why one reducer always finishes last](#why-one-reducer-always-finishes-last).

**Partition** — the corpus split into chunks, sharded by hashing the source URL.
64 partitions means the corpus is in 64 pieces. A partition is both the unit that
moves between machines and the unit of work handed out.

### The processing

**Map stage** — read pages, emit one edge per link. Fully parallel: every
partition can be processed independently, on any machine.

**Shuffle** — send each edge to the machine responsible for its target. This is
the regrouping step, and it is the whole point of MapReduce. Destination is
`hash(targetUrl) % reducerCount`, so every edge pointing at the same target lands
on the same machine.

**Reduce stage** — collect what arrived and group it. Here that means building
`Map<target, Set<source>>`.

Note that the map stage shards by **source** and the shuffle shards by
**target**. Moving each edge from one grouping to the other is the actual work.

### The machines

**Controller** — one process that tracks which machines are alive, decides where
data lives, hands out work, and writes the final output. Holds no data itself.

**Node** (also: worker) — a process that stores data, runs map tasks, and
possibly acts as a reducer. Every node does all three. There is no separate
"mapper machine" and "reducer machine."

**Reducer** — a node additionally assigned to hold part of the index for a job.
It keeps its slice in memory and receives shuffle traffic from every mapper, so
it is the role that costs RAM.

**Replication factor (RF)** — how many machines hold a copy of each partition.
RF=2 means every partition is on two machines, so one can die without data loss.

**Locality** — running a task on a machine that already holds the data, so
nothing has to be copied first.

### The knobs

**`--slots`** — how many map tasks one node runs at the same time. **The most
important setting**, for reasons in
[Why slots matter and threads do not](#why-slots-matter-and-threads-do-not).
Defaults to cores/2.

**`--parserThreads`** — how many threads work inside a single map task. Defaults
to 2. Raising it does far less than you would expect.

**`--reducers`** — how many nodes hold the index for a job. More reducers means
less memory each and more parallelism in the reduce stage.

**`--replication`** — the RF above. Default 2.

### Fault handling

**Straggler** — a task taking much longer than the others, usually because its
machine is slow rather than because its work is bigger.

**Speculative execution** — spotting a straggler, starting a second copy of the
task elsewhere, and taking whichever finishes first.

**Gossip** — nodes periodically telling each other who they can still reach, so a
machine that has frozen gets noticed even when its socket is still open.

---

## Cluster setup

One machine runs the controller:

```bash
java -jar linkmesh.jar controller --port 9000 --replication 2 --reducers 2
```

Every other machine joins with one flag:

```bash
java -jar linkmesh.jar worker --controller 192.168.1.10:9000
```

Node name, network address, port and data directory are all worked out for you. A
node reports what it already has on disk when it joins, so restarting one is
cheap.

Reducers hold their slice of the index in memory, so put them where the RAM is:

```bash
java -Xmx8g -jar linkmesh.jar worker --controller HOST:9000 --role reducer
java -jar linkmesh.jar worker --controller HOST:9000 --role mapper
```

Firewall rules and the full setup guide: [docs/OPERATING.md](docs/OPERATING.md).

## Loading data

LinkMesh does not crawl. It indexes pages you give it, in one of three formats.

**Wikipedia dumps:**

```bash
java -jar linkmesh.jar ingest --controller HOST:9000 \
  --wikipedia simplewiki-NS0-ENTERPRISE-HTML.json.tar.gz --partitions 64
```

**WARC files** (Common Crawl, or your own `wget --warc-file` crawl):

```bash
java -jar linkmesh.jar ingest --controller HOST:9000 --warc segment.warc.gz --partitions 64
```

**Plain edge lists** (SNAP and other graph benchmarks):

```bash
java -jar linkmesh.jar ingest --controller HOST:9000 --edges web-BerkStan.txt.gz --partitions 32
```

Then run a job:

```bash
java -jar linkmesh.jar submit --controller HOST:9000
java -jar linkmesh.jar status --controller HOST:9000
```

### Why WARC and not a folder of .html files

A saved `page.html` has lost the address it came from. Real HTML is full of
relative links like `href="/about"`, and you cannot turn that into a real URL
without knowing which page it was on. WARC records carry a `WARC-Target-URI`
header with exactly that.

Feeding in bare HTML files would still *run*. It would just produce a wrong index
without complaining, which is worse than failing.

### Why URLs get normalized

These four links all point at the same page:

```
https://beta.example/home
https://beta.example/home#section
HTTPS://BETA.EXAMPLE/home
https://beta.example:443/home
```

Left alone they become four separate keys and every count is wrong. The ingester
lowercases the host, drops the fragment, drops the default port, resolves
relative links against the page URL (honouring `<base href>`), and decodes HTML
entities. CI asserts an exact expected index from a test file covering all of
these cases.

### Why same-site links are dropped by default

On the open web, most links on a page are the site's own navigation: header,
footer, privacy policy. Index those and the top of your results is
`somesite.com/privacy`. The default keeps only links that cross between sites.

Wikipedia is the exception, since article-to-article links are all same-site, so
the `--wikipedia` path keeps internal links automatically.

---

## Results, and why they came out that way

Two machines were used, and numbers are labelled with which:

- **Laptop**: 8 cores, 5.7 GB RAM, Windows 11, OpenJDK 21
- **CI runner**: GitHub Actions, 4 cores AMD EPYC 7763, 15 GB RAM

Every configuration below produced byte-identical output, checked by SHA-256 over
the sorted index. That is the part that matters most. Timings are secondary.

### The datasets

| dataset | pages / nodes | edges | source |
|---|---|---|---|
| Simple English Wikipedia | 164,601 | 7,049,474 | Wikimedia Enterprise HTML dump |
| web-BerkStan | 685,230 | 7,600,595 | [SNAP](https://snap.stanford.edu/data/) |
| soc-LiveJournal1 | 4,847,571 | 68,993,773 | [SNAP](https://snap.stanford.edu/data/) |

The SNAP graphs publish their exact edge counts, which doubles as a correctness
check: extraction produced exactly 7,600,595 and exactly 68,993,773.

### Headline run

164,601 Wikipedia articles, laptop, one node with `--slots 4`:

```
map stage      10.2 s
total job      16.8 s
pages          164,601
links          7,049,474
backlink keys  273,052
max fan-in     19,242
```

About 16,000 pages/sec through the map stage. Output is 358 MB of TSV.

### Public graph datasets, on the CI runner

Both SNAP graphs, one node, `--reducers 1`, varying `--slots`. Run it yourself
from the Actions tab, or:

```bash
gh workflow run benchmark.yml -f dataset=web-BerkStan -f partitions=32
```

**web-BerkStan** — 685,230 nodes, 7,600,595 edges, 617,094 keys, max fan-in
84,208. Heap 5g, 32 partitions, median of 3:

| slots | map stage | speedup |
|---|---|---|
| 1 | 16,503 ms | 1.00x |
| 2 | 8,482 ms | 1.95x |
| 4 | 5,095 ms | 3.24x |

**soc-LiveJournal1** — 4,847,571 nodes, 68,993,773 edges, 4.3M sources. Heap
11g, 128 partitions, median of 2:

| slots | map stage | speedup |
|---|---|---|
| 1 | 115,028 ms | 1.00x |
| 2 | 93,821 ms | 1.23x |
| 4 | 83,477 ms | 1.38x |

69M edges indexed in 109 s end to end, roughly 830,000 edges/sec through the map
stage.

The two datasets reach very different speedups, 3.24x against 1.38x. The obvious
reading is that the bigger graph scales worse. That reading is wrong, and
[the controlled experiment](#does-parallel-speedup-get-worse-on-bigger-graphs-no)
below shows why: these two runs also differ in partition count, heap and trial
count, and at equal size the gap is still there.

### Why slots matter and threads do not

Same corpus, one node, changing only `--slots` (laptop, 61,322 articles):

| slots | map stage | speedup |
|---|---|---|
| 1 | 12,378 ms | 1.00x |
| 2 | 6,372 ms | 1.94x |
| 4 | 3,618 ms | 3.42x |
| 8 | 3,621 ms | 3.42x |

Now the same machine with **slots stuck at 1** but the parser pool widened from 2
threads to 8: 12,376 ms. Essentially unchanged, despite four times the threads.

**Why.** A map task is not only parsing. Each one:

1. creates a ForkJoinPool and a parser thread pool
2. scans its partition directory
3. parses the files it found
4. flushes its remaining shuffle batches
5. shuts both pools down

Steps 1, 2, 4 and 5 are serial per task, and no number of parser threads shortens
them. Step 3 is smaller than you would think: a partition here holds about 11
files, so with 8 parser threads most threads get one file and then go idle.

With `slots=1` the node runs that whole sequence once per partition, 24 times
back to back, paying the serial part 24 times with nothing overlapping it. With
`slots=4`, four sequences run at once, so one task's pool setup overlaps another
task's parsing.

**The unit of useful parallelism here is the task, not the thread.** It flattens
at 4 because 4 slots x 2 parser threads is 8 threads on 8 cores.

This is also a real inefficiency, not just a tuning fact. Building thread pools
per task is wasteful, and reusing them across tasks on a node would shrink the
gap. Not done yet.

### Adding real machines: the one test a single host cannot do

Every result above runs on one machine, where splitting into more node processes
only ever costs. To find out whether adding *machines* helps, each node needs its
own hardware. GitHub Actions gives every job in a matrix its own VM, 4 cores and
15 GB, so the nodes are put on separate runners and joined over a Tailscale
network. Setup is in [docs/OPERATING.md](docs/OPERATING.md).

web-BerkStan, 7.6M edges, `--shuffleBatch 8192`. Each machine runs one node, and
`--slots` is how many map tasks that node runs at once, so the work in flight is
machines x slots:

| machines | slots each | tasks at once | map stage | vs 1x1 |
|---|---|---|---|---|
| 1 | 1 | 1 | 18,655 ms | 1.00x |
| 2 | 1 | 2 | 14,049 ms | 1.33x |
| 4 | 1 | 4 | 8,731 ms | 2.14x |
| 1 | 4 | 4 | 4,631 ms | 4.03x |
| 2 | 4 | 8 | 3,891 ms | 4.79x |
| 4 | 4 | 16 | 2,681 ms | **6.96x** |

Read down either slots column and adding machines helps: 1.00 to 1.33 to 2.14 at
one slot each, 1.00 to 1.19 to 1.73 at four. Read across and filling a machine's
slots helps more.

Every one of the six produced the identical 617,094-key index.

**Fill a machine before adding another.** Compare the two rows with 4 tasks in
flight:

| layout | tasks | map stage |
|---|---|---|
| 1 machine x 4 slots | 4 | 4,631 ms |
| 4 machines x 1 slot | 4 | 8,731 ms |

Same concurrency, and the single machine is **1.89x faster**. Spreading the same
work over more machines means shuffle traffic crosses the network instead of
staying in one process. So use the cores you already have first, then add
machines for the parallelism you cannot get locally, which is what the 4x4 row
does.

Scaling is sublinear: 16 tasks give 6.96x, not 16x. `--reducers 2` caps the
reduce side however many mappers feed it, and at 7.6M edges the fixed per-run
costs, distributing data and collecting output across the network, are a visible
share of a run this short.

### The bug that only a real network could reveal

The first multi-machine attempt was a disaster, and worth keeping:

| workers | shuffle batch | map stage |
|---|---|---|
| 1 | 512 | 6,341 ms |
| 2 | 512 | **39,976 ms** |
| 4 | 512 | 23,697 ms |

Two machines were **six times slower** than one. Not overhead; something broken.

**Why.** `ShuffleWriter` sends a batch and blocks for the reply. On one machine
the reducer is the same process and that reply costs almost nothing, so a batch
size of 512 was invisible. Across real machines every batch became a network
round trip: 7.6M edges at 512 records is about 14,800 batches, half of them
crossing the wire, so roughly 7,000 blocking round trips through a WireGuard
tunnel between two datacenters. That is the entire 34-second gap.

Raising only the batch size, changing nothing else:

| workers=2, shuffle batch | map stage |
|---|---|
| 512 | 39,976 ms |
| 8192 | 3,891 ms |

**10.3x faster**, and it turned "two machines are 6x slower" into "two machines
are 1.19x faster." The default is now 4096 rather than 512.

The real fix is to stop blocking on every batch and pipeline the sends, which
would make the batch size much less critical. Not done yet.

This is the argument for testing on real hardware. The bug was invisible on
loopback, invisible on one runner, and invisible in every benchmark above it,
because they all had a shuffle whose round trips were free.

### Why splitting one machine into more nodes is slower

Same total work in flight (4 concurrent tasks), just divided across more node
processes on **one** machine:

| layout | laptop | CI runner |
|---|---|---|
| 1 node x 4 slots | 3,324 ms (1.00x) | 5,021 ms (1.00x) |
| 2 nodes x 2 slots | 4,288 ms (0.78x) | 5,716 ms (0.88x) |
| 4 nodes x 1 slot | 5,004 ms (0.66x) | 6,015 ms (0.83x) |

Splitting one machine into four node processes made it **34% slower** on the
laptop and 17% slower on the CI runner. Both agree on the direction.

**Why.** Four JVMs on one box do not create four machines' worth of CPU. What
they do add is cost:

- Four separate heaps, four sets of GC threads, four JIT compilers warming up
- Shuffle that was an in-process method call becomes a TCP round-trip with
  serialization at both ends, even over loopback
- Four heartbeat streams, a gossip mesh, four sets of scheduler bookkeeping
- Four working sets competing for the same CPU cache

The laptop takes the bigger hit because it has less RAM and slower cores, so the
fixed per-JVM overhead is a larger share of what it has.

**What this means in practice.** Distribution is not a speed feature. It buys
fault tolerance and the ability to use machines you would otherwise not have. If
you have one machine, run one node and raise `--slots`. Showing that adding
*machines* speeds things up needs machines with their own cores, which a single
host cannot demonstrate however it is configured.

### Does parallel speedup get worse on bigger graphs? No

It looked that way at first. web-BerkStan (7.6M edges) reached 3.24x on 4 slots
while soc-LiveJournal1 (69M edges) managed only 1.38x, and the obvious story was
that the reduce stage is a serial fraction that grows with the data.

That story is wrong, and the arithmetic gives it away. If map and reduce both
scale linearly with edge count their ratio is constant, so speedup should not
move. Worse, the *serial* run of the big graph was **faster** per edge (1.67 µs
vs 2.17 µs), which is the opposite of a growing serial fraction and exactly what
you would expect from amortizing fixed overhead.

The comparison was also not an experiment. It changed dataset, partition count
(32 vs 128), heap (5g vs 11g) and trial count all at once.

So here is the controlled version: **one** dataset, fed progressively more of
itself, with heap fixed at 10g, one node, one reducer, same machine. Corpora are
sharded by hash, so taking N partitions is a uniform sample of the graph.

| edges | vs smallest | slots=1 | slots=2 | slots=4 | speedup at 4 | GC at 4 slots |
|---|---|---|---|---|---|---|
| 7.4M | 1.0x | 9,362 ms | 8,778 ms | 7,283 ms | 1.29x | 25.4% |
| 17.2M | 2.3x | 24,253 ms | 19,332 ms | 16,378 ms | 1.48x | 28.5% |
| 34.5M | 4.7x | 50,424 ms | 40,142 ms | 35,611 ms | 1.42x | 29.4% |
| 69.0M | 9.3x | 106,082 ms | 86,323 ms | 79,627 ms | 1.33x | 31.1% |

**Speedup is flat.** Across a 9.3x increase in data it goes 1.29, 1.48, 1.42,
1.33 with no trend, and the smallest graph is the *worst* of the four. Scaling
does not degrade with size.

Two things are true and worth separating:

**Per-edge cost grows slowly with size.** At one slot it rises from 1.268 to
1.538 µs/edge, 21% over 9.3x more data. That is the memory hierarchy: the
reducer's hash table outgrows cache, so inserts become random DRAM accesses. It
is real, but it is sub-logarithmic and it hits the serial and parallel runs
alike, which is exactly why speedup stays put.

**The 3.24x vs 1.38x gap was a dataset difference, not a size difference.** At
comparable size, 7.4M edges of LiveJournal gives 1.29x where 7.6M edges of
BerkStan gives 3.24x. Same scale, 2.5x apart. LiveJournal is a social graph with
4.8M nodes; BerkStan is a web graph with 685k. For a similar edge count that is
far more distinct keys, so a much larger hash table, and GC sits at 25-31% of
wall time on these runs.

That key-count explanation is a hypothesis, not a measurement. What is measured
is that graph *shape* matters much more than graph *size*, and that raising the
heap does not help: at fixed data size, 600m / 1200m / 2400m gave 1.59x / 1.44x /
1.49x, flat within noise. Whatever the limit is, it is not collector pressure
that more headroom relieves.

Reproduce with `./scripts/scaling-vs-size.sh` or:

```bash
gh workflow run scaling.yml -f dataset=soc-LiveJournal1 -f heap=10g
```

### Why one reducer always finishes last

Fan-in distribution from the Wikipedia run:

| backlinks | number of keys |
|---|---|
| 1 | 91,504 |
| 2-10 | 104,600 |
| 11-100 | 62,982 |
| 101-1,000 | 13,783 |
| 1,000+ | 183 |

Most keys have one or two backlinks. A handful have tens of thousands.
`United_States` had 19,242. On web-BerkStan the biggest key had **84,208**.

**Why this hurts.** Edges go to reducers by `hash(target) % reducers`, which
spreads *keys* evenly. It does not spread *key sizes*, and key sizes here span
five orders of magnitude. Whichever reducer happens to own `United_States` does
far more work than the others and finishes last while the rest sit idle.

Nothing here fixes that. It reports `max_fan_in` so the skew is at least visible
instead of showing up as a mysteriously slow reducer.

### Where it runs out of memory

soc-LiveJournal1's 69M edges index fine on one node with an 11 GB heap. The same
data split across 4 nodes capped at 5 GB each **fails**: with 2 reducers, each
holds about 34.5M edges, and that does not fit.

So the ceiling is roughly **35M edges per reducer per 5 GB of heap**, about 150
bytes per edge.

**Why 150 bytes for what is conceptually two pointers.** For each key the store
holds a String, a hash map entry, and a `Set` — and that Set is itself a hash map
costing roughly 200 bytes even when it holds a single element. Each edge inside it
then costs another node object. With millions of keys, the per-key overhead
dominates.

Source URLs are interned, which turned out to be necessary rather than an
optimization. A page with 43 outbound links contributes its own URL to 43
different sets, and each batch decode allocates a fresh String. Before interning,
the store held one String per *edge* instead of one per *page*, and 2.6M edges
would not fit in a 512 MB heap. After interning, they do.

To go past the ceiling: add reducers, or write reducers that spill to disk, which
this does not do.

### Fault tolerance

```bash
./scripts/failure-demo.sh
```

Runs a job normally, then runs it again killing a node partway through, and
compares. The sorted output hash is **identical**.

```
WARN  [cluster] node node-3 declared DEAD (control connection closed)
WARN  [sched]   part-004 will be rescheduled, lost with node node-3
INFO  [control] replicating part-001 from node-1 to node-2
```

**Why it survives.** Two reasons. Every partition was on two machines (RF=2), so
the dead node's data still exists somewhere. And the half-finished work the dead
node already shipped is harmless, because re-running the task re-sends the same
edges, and adding an edge to a `Set` twice does nothing.

**Why the death is noticed in milliseconds** rather than after a timeout: each
node holds one TCP connection to the controller that is never reused for anything
else. When the process dies the operating system closes that socket, and a closed
socket is unambiguous. Gossip between nodes covers the other case, a machine that
has frozen but whose socket is still open, which the controller cannot detect on
its own.

### Stragglers

```bash
./scripts/straggler-demo.sh
```

One node is artificially slowed:

```
map stage without speculation : 4332 ms
map stage with speculation    : 3533 ms

INFO [sched] straggler: part-009 on node-3 at 1036 ms vs median 73 ms,
             starting backup on node-1
INFO [sched] backup attempt won for part-009, cancelling the original
```

The backup finished in 55 ms because node-1 already had a copy of that partition,
so it started work immediately instead of copying data first.

**Why running the same task twice is safe here**, with no locking and no commit
protocol: the reducer stores `Map<target, Set<source>>`. Applying the edge
`(A, B)` twice gives exactly the same set as applying it once. Both attempts
converge on an identical index however their output interleaves, so the loser can
simply be cancelled and its partial output ignored.

**This is a property of this workload, not a general result.** A reduce that
summed or counted would double-count, and would need the losing attempt properly
fenced off before its output could be discarded.

### Reproducing

Correctness runs on every push via GitHub Actions: a 3-node run and a 1-node run
must produce the same index, killing a node mid-job must not change the result,
speculation must not change the result, and a hand-built WARC must normalize to an
exact expected index.

Benchmarks run on a clean runner, on demand:

```bash
gh workflow run benchmark.yml -f dataset=web-BerkStan -f partitions=32
```

Locally:

```bash
java -jar build/linkmesh.jar ingest --edges web-BerkStan.txt.gz --out build/corpus --partitions 32
CORPUS=build/corpus MODE=slots ./scripts/benchmark.sh
CORPUS=build/corpus MODE=fixed ./scripts/benchmark.sh
```

---

## Sizing

Reducer memory is the binding constraint, at roughly 150 bytes per edge:

| edges | total reducer heap | example |
|---|---|---|
| 2.6M | ~400 MB | 60k Wikipedia articles |
| 7.0M | ~1.2 GB | 165k Wikipedia articles |
| 69M | ~11 GB | soc-LiveJournal1 |

Divide by reducer count for per-machine heap. Two reducers means each needs about
half.

## Commands

```
controller   run the controller
worker       run a node (storage + compute + reducer)
ingest       load a corpus: --wikipedia, --warc, --edges, --prepared,
             or --out to parse to local disk without a cluster
gen          generate a synthetic corpus
submit       start a job
status       cluster and job state
cancel       cancel the running job
```

`java -jar linkmesh.jar help` lists every flag. Any flag can also be set from
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

Inside one node, a single map task looks like this:

```
partition directory
     |  ForkJoinPool, work-stealing directory scan
     v
BoundedTaskQueue          backpressure, instrumented
     |  fixed thread pool
     v
parse page, emit edges -> batched shuffle to reducers
```

Three different concurrency tools, each picked for its job. ForkJoin for scanning
the directory tree, because the tree is uneven and work stealing balances it with
no manual splitting. A fixed thread pool for parsing, because those tasks are
uniform and CPU-bound so stealing would only add overhead. Virtual threads for
network connections, because there are many and they spend almost all their time
idle.

[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) covers the protocol, storage layout,
placement planner and failure detection in detail.

```
src/main/java/linkmesh/
  Main.java        CLI
  proto/           framed protocol, pooled connections, archive format
  cluster/         membership, placement map, planner
  storage/         local replica store
  controller/      scheduling, speculation, job state
  worker/          map pipeline, bounded queue, shuffle, reducer, gossip
  ingest/          Wikipedia, WARC and edge-list readers, URL normalization
```

45 files, about 6,100 lines.

## Limits

- **The controller is a single point of failure for scheduling.** Data survives
  it; a job running at the time does not.
- **Reducer state is in memory and not replicated.** Losing a reducer mid-job
  fails the job.
- **Reducer skew is not handled.** One reducer gets the hot keys and finishes
  last.
- **Thread pools are rebuilt per task**, which is why `--slots` matters so much.
- **Partition archives are held in memory during transfer.** Prefer more
  partitions over bigger ones.
- **No authentication, no TLS.** Trusted networks only.
- **Link extraction is regex-based**, not a real HTML parser.

## Requirements

Java 21 or newer. Bash for the helper scripts.
