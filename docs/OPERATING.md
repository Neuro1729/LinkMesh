# Operating LinkMesh

How to set up machines, assign roles, add capacity, and run it on real web pages.

---

## First, one thing that avoids a lot of confusion

**LinkMesh does not crawl the internet.** It never goes out and fetches pages on
its own, and it has no notion of "searching the web."

It indexes **exactly the pages you hand it**, and nothing else. You supply a set
of already-downloaded web pages (a WARC file), it extracts the links, and it
builds the backlink index for that set. If you give it 50,000 pages, it looks at
50,000 pages. It will never discover a 50,001st.

```
   You download pages   ->   LinkMesh indexes them   ->   backlink index
   (Common Crawl, wget)      (this project)               (output TSV)
```

So the answer to "how many pages does it search on the internet" is: **none.** The
number it processes is entirely controlled by you, via how much WARC data you
feed it and the `--maxPages` flag. See [How many pages](#how-many-pages) below.

---

## The machine roles

There are only **two kinds of process**, and one of them is optional to think about.

| Process | How many | What it does |
|---|---|---|
| **Controller** | Exactly one | Tracks membership, decides where replicas live, schedules tasks, writes the final output. Holds no data. |
| **Node** | As many as you like | Stores partition replicas, runs map tasks, and *may* also serve as a reducer. |

The important thing, and it is different from Hadoop: **there is no separate
"mapper machine" and "reducer machine."** Every node is always a storage node and
always able to run map tasks. Being a reducer is an *additional* job that the
controller hands to some of your nodes when a job starts.

That is deliberate. It is what makes locality-aware scheduling possible — the
controller can put a map task on a machine that already has the bytes on its
disk, so the common case moves no data across the network at all.

---

## 1. Set up the controller machine

Pick one machine. This one needs no data disk and little RAM; it is a scheduler.

```bash
java -jar linkmesh.jar controller \
  --port 9000 \
  --replication 2 \
  --reducers 2 \
  --output output/backlinks.tsv
```

It prints the exact command to paste on every other machine:

```
  LinkMesh controller ready on 192.168.1.10:9000
  Join a node with:  java -jar linkmesh.jar worker --controller 192.168.1.10:9000
```

Leave it running. It stays up across many jobs.

| Flag | Meaning |
|---|---|
| `--port 9000` | Where nodes connect in |
| `--replication 2` | Copies of every partition. `2` survives one machine dying; `3` survives two |
| `--reducers 2` | How many nodes hold reducer state for a job |
| `--output PATH` | Where the final index is written — **on the controller machine** |

> The final output file lands on the controller, not on the nodes. Every reducer
> streams its slice of the index back to it.

---

## 2. Add machines

On each other machine, the whole setup is one line:

```bash
java -jar linkmesh.jar worker --controller 192.168.1.10:9000
```

That is genuinely all. The node works out its own id (the hostname), its LAN
address, its listen port (7100), and its data directory
(`linkmesh-data/<hostname>`).

You can add machines at **any time** — before ingest, after ingest, or while a
job is running. A machine that joins an already-loaded cluster is noticed within
seconds, and the controller starts shipping it replicas to rebalance:

```
INFO  [control] node laptop-4 joined as AUTO with 0 partitions (0 B)
INFO  [control] replicating part-003 from desk-1 to laptop-4
INFO  [control] replicating part-007 from desk-2 to laptop-4
```

### Removing a machine

Just stop it (Ctrl-C or kill). The controller notices in milliseconds,
re-replicates whatever it held from the surviving copies, and reschedules any
task it was running. No command needed.

### Restarting a machine

Start it again with the same `--id`. It keeps the replicas already on its disk
and reports them on rejoin, so it does not refetch gigabytes it already has.

---

## 3. Choosing which machine is a reducer

Every node can run map tasks. Being a **reducer** is the role with a real
resource cost, so it is the one worth placing deliberately:

- A reducer holds the whole backlink index **for its slice of the keyspace in
  memory** for the duration of the job.
- Every mapper on every machine sends it shuffle traffic.

So: **put reducers on your machines with the most RAM.**

Use `--role`:

```bash
# Big machine: 32 GB RAM. Ask for reducer duty and give it a large heap.
java -Xmx16g -jar linkmesh.jar worker --controller 192.168.1.10:9000 \
  --id big-1 --role reducer

# Small machine: 8 GB laptop. Map only; never hold reducer state.
java -jar linkmesh.jar worker --controller 192.168.1.10:9000 \
  --id laptop-2 --role mapper

# Don't care: let the controller decide. This is the default.
java -jar linkmesh.jar worker --controller 192.168.1.10:9000 --id any-3
```

| `--role` | Effect |
|---|---|
| `reducer` | Preferred when the controller picks reducers |
| `mapper` | Never used as a reducer (unless there is literally no other machine) |
| `auto` | Eligible, no preference. **Default** |

The controller picks reducers in this order: `reducer` nodes first, then `auto`
nodes, and `mapper` nodes only as a last resort — in which case it warns you by
name so you can fix the setup:

```
WARN [control] using mapper-only node laptop-2 as a reducer: only 1 node(s) are
     reducer-eligible but --reducers is 2
```

Confirm who got the job in the controller log when the job starts:

```
INFO [control] job demo started: 64 partitions, 4 nodes, 2 reducers
     ([ReducerAssignment[reducerId=0, nodeId=big-1, ...],
       ReducerAssignment[reducerId=1, nodeId=big-2, ...]])
```

And `status` shows every machine's role at a glance:

```
node    big-1     192.168.1.11:7100   ALIVE   REDUCER   16 partitions   0/1 tasks   4.1 GB
node    laptop-2  192.168.1.12:7100   ALIVE   MAPPER    16 partitions   1/1 tasks   4.1 GB
node    any-3     192.168.1.13:7100   ALIVE   AUTO      16 partitions   1/1 tasks   4.1 GB
```

### How many reducers?

`--reducers N` on the controller. Rough guidance:

- **More reducers** = less memory each, more parallelism in the reduce stage.
- **Fewer reducers** = fewer, larger shuffle destinations.

Start with 2, and raise it if a reducer runs out of heap. It cannot exceed your
node count.

---

## 4. Open the firewall

Three traffic flows. The third one is the one people forget, and forgetting it
gives you a cluster that joins and assigns work and then dies on the first
shuffle.

| Flow | Open on | Port |
|---|---|---|
| node → controller | controller machine | 9000 |
| controller → node | every node | 7100 |
| **node → node** | **every node** | **7100** |

Nodes talk *directly to each other* for gossip, shuffle, and replica transfers.
They do not relay through the controller.

```powershell
# Windows, on each node
New-NetFirewallRule -DisplayName "linkmesh" -Direction Inbound `
  -Protocol TCP -LocalPort 7100 -Action Allow
```

```bash
# Linux, on each node
sudo ufw allow 7100/tcp
```

Check reachability before you start: `Test-NetConnection 192.168.1.11 -Port 7100`

### If a node advertises the wrong address

Multi-homed machines, VPNs, and containers can make address detection guess
wrong. Override it:

```bash
java -jar linkmesh.jar worker --controller 192.168.1.10:9000 --advertise 192.168.1.12
```

Symptom of a wrong guess: the node joins fine, but tasks assigned to it time out.
Check `status` — the address shown there is the one everyone else will dial.

---

## 4b. Tuning a machine

`--slots` sets how many map tasks a node runs at once, and on a multi-core
machine it is the setting that matters most. It defaults to cores/2.

Measured on an 8-core laptop, 61k articles:

| slots | map stage |
|---|---|
| 1 | 12,378 ms |
| 2 | 6,372 ms |
| 4 | 3,618 ms |
| 8 | 3,621 ms |

`--parserThreads` sets the threads inside one task and moves the needle far less,
because a single partition often does not have enough files to keep a wide pool
busy. Leave it at 2 and raise `--slots` instead.

Total threads on a node is roughly `slots x parserThreads`. Aim for that to be
near the core count.

Note that running several node processes on one machine is slower than running
one node with more slots. Extra JVMs on the same box compete for the same cores
and add cross-process shuffle. Use multiple nodes because you have multiple
machines, or because you want replication, not to speed up a single host.

## 5. Running several nodes on one machine

Useful for testing. Give each one its own id, port, and data directory:

```bash
java -jar linkmesh.jar worker --controller 127.0.0.1:9000 --id n1 --port 7101 --data data/n1
java -jar linkmesh.jar worker --controller 127.0.0.1:9000 --id n2 --port 7102 --data data/n2
```

Or just use the script: `NODES=3 ./scripts/cluster-up.sh`

---

## 6. Getting real web pages

LinkMesh reads **WARC** files (`.warc` or `.warc.gz`). That is the standard web
archive format, and it is required for one specific reason: every record carries
a `WARC-Target-URI` header saying **which URL the page was downloaded from**.

Without that, a page is unusable. Real HTML is full of relative links:

```html
<a href="/about">        <a href="../docs/x.html">        <a href="#top">
```

None of those can be turned into a real URL unless you know where the page came
from. A bare `page.html` sitting on your disk has lost that information forever.
This is why "just point it at a folder of HTML files" is not offered — it would
silently produce a wrong index.

### Option A: Common Crawl (recommended)

Free, enormous, already in WARC format. Browse an index at
<https://data.commoncrawl.org/crawl-data/> and download one segment file:

```bash
curl -O https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-10/segments/.../warc/CC-MAIN-....warc.gz
```

One segment file is roughly 1 GB and holds on the order of 30,000–50,000 pages.

### Option B: crawl a slice yourself

`wget` writes WARC directly:

```bash
wget --warc-file=mysite --recursive --level=3 --domains=example.com \
     --wait=1 --delete-after https://example.com/
```

`--wait=1` is politeness, not decoration. Respect `robots.txt` and rate limits
when crawling anything you do not own.

---

## 7. Ingest and run

```bash
# Push the corpus into the cluster (shards it, replicates it, distributes it)
java -jar linkmesh.jar ingest --controller 192.168.1.10:9000 \
  --warc segment.warc.gz --partitions 64 --maxPages 100000

# Run the job
java -jar linkmesh.jar submit --controller 192.168.1.10:9000 --job crawl-1
```

Ingest tells you exactly what it found:

```
  records scanned : 48,213
  pages indexed   : 31,884
  links extracted : 412,556
  partitions      : 64 (1.9 GB pushed including replicas)
  elapsed         : 3 m 12 s
```

You only ingest once. After that you can run `submit` as many times as you like
against the same data.

### Ingest flags

| Flag | Meaning |
|---|---|
| `--warc FILE\|DIR` | A WARC file, or a directory of them (all are read) |
| `--partitions 64` | How many pieces to split the corpus into |
| `--maxPages N` | **Stop after N pages.** Your main size control |
| `--includeInternal` | Keep same-site links (off by default — see below) |
| `--prepared DIR` | Push an already-partitioned directory instead of a WARC |

### Why same-site links are dropped by default

On real HTML, roughly **90% of all links are site navigation chrome** — every
page on a site links to its own header, footer, and privacy policy. Index those
and your result is a huge, boring list dominated by `somesite.com/privacy`.

Dropping them cuts the edge count by an order of magnitude and leaves the links
that actually mean something: one site pointing at another. Pass
`--includeInternal` if you specifically want site-internal structure.

### Picking `--partitions`

Aim for partitions of roughly **50–200 MB each**. More, smaller partitions is
the safer direction: it gives the scheduler finer-grained work to balance, and
partition archives are held in memory during transfer.

| Corpus | Suggested `--partitions` |
|---|---|
| 10k pages | 16 |
| 100k pages | 64 |
| 500k pages | 256 |

---

## How many pages

You control this entirely. The binding constraint is **reducer memory**, because
reducer state is held in RAM — roughly 100–200 bytes per unique edge.

| Pages | External edges | Total reducer heap needed | Verdict |
|---|---|---|---|
| 10,000 | ~200k | negligible | Instant |
| **100,000** | **~2M** | **~0.5 GB** | **Comfortable. Good target** |
| 500,000 | ~10M | ~2 GB | Fine with `-Xmx4g` reducers |
| 1,000,000 | ~20M | 3–5 GB | Tight. Use more reducers |
| 3,000,000+ | 60M+ | 10 GB+ | Will run out of heap |

That heap total is **divided across your reducers**, so two reducers each need
about half. To go bigger, raise `--reducers` and give them larger heaps:

```bash
java -Xmx8g -jar linkmesh.jar worker --controller ... --role reducer
```

Start conservative and work up:

```bash
java -jar linkmesh.jar ingest --controller ... --warc segment.warc.gz --maxPages 20000
```

If that completes comfortably, raise `--maxPages` and re-ingest.

Going beyond a few million edges per reducer needs reducers that spill sorted
runs to disk, which this version does not do. That is the honest ceiling.

---

## 8. Watching a job

```bash
java -jar linkmesh.jar status --controller 192.168.1.10:9000
```

```
phase=MAP
nodes=4
partitions=64
replicationFactor=2
progress=37/64
speculativeAttempts=2
speculativeWins=1
node    big-1  192.168.1.11:7100  ALIVE  REDUCER  32 partitions  1/1 tasks  1.9 GB
...
underReplicated=0
```

The two lines to watch:

- **`underReplicated=0`** — every partition has its full replica count. If this
  is non-zero for more than a few seconds, a transfer is failing.
- **`progress=37/64`** — map stage progress.

Cancel a running job with:

```bash
java -jar linkmesh.jar cancel --controller 192.168.1.10:9000
```

### Reading the result metrics

```
METRIC pages=31884              pages actually parsed
METRIC links_emitted=412556     edges emitted by mappers
METRIC backlink_keys=98211      distinct URLs that received at least one link
METRIC backlink_edges=401203    edges after set deduplication
METRIC max_fan_in=8842          most backlinks any single URL received
METRIC speculative_attempts=3   backup attempts started for stragglers
METRIC speculative_wins=2       times the backup beat the original
```

`max_fan_in` is the one worth watching on real web data. Web links follow a power
law, so a handful of URLs collect an enormous share of all backlinks. Hash
partitioning spreads *keys* evenly but not key *sizes*, so the reducer holding
that giant key will finish last. A large `max_fan_in` explains a slow reduce
stage.

---

## Worked example: 4 machines, real crawl data

```
192.168.1.10   controller        (any small machine)
192.168.1.11   big-1, 32 GB      reducer
192.168.1.12   big-2, 32 GB      reducer
192.168.1.13   laptop-3, 8 GB    mapper only
192.168.1.14   laptop-4, 8 GB    mapper only
```

**On 192.168.1.10:**
```bash
java -jar linkmesh.jar controller --port 9000 --replication 2 --reducers 2 \
  --output output/backlinks.tsv
```

**On 192.168.1.11 and .12:**
```bash
java -Xmx16g -jar linkmesh.jar worker --controller 192.168.1.10:9000 --role reducer
```

**On 192.168.1.13 and .14:**
```bash
java -jar linkmesh.jar worker --controller 192.168.1.10:9000 --role mapper
```

**From anywhere:**
```bash
java -jar linkmesh.jar status  --controller 192.168.1.10:9000   # confirm 4 nodes
java -jar linkmesh.jar ingest  --controller 192.168.1.10:9000 \
     --warc segment.warc.gz --partitions 64 --maxPages 100000
java -jar linkmesh.jar submit  --controller 192.168.1.10:9000 --job crawl-1
```

Result appears at `output/backlinks.tsv` **on 192.168.1.10**:

```
https://example.com/article    https://a.net/x,https://b.org/y,https://c.io/z
```

Each line: a URL, a tab, then the comma-separated list of pages linking to it.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Node joins, tasks time out | Advertising an unreachable address | `--advertise <lan-ip>` |
| Job dies at first shuffle | node↔node port closed | Open 7100 between nodes, not just to the controller |
| `controller unreachable, retrying` | Controller down or port 9000 blocked | Start controller; check firewall |
| `cluster holds no partitions` | Nothing ingested yet | Run `ingest` first |
| `N partitions have no live replica` | Too many machines died at once | Restart a dead node, or re-ingest |
| `OutOfMemoryError` on a reducer | Corpus too big for reducer heap | Raise `--reducers`, add `-Xmx`, or lower `--maxPages` |
| `no pages extracted` | Input has no HTML responses | Confirm it is a real WARC with `response` records |
| One reducer far slower | Power-law skew (`max_fan_in`) | Expected on web data; more reducers helps a little |

---

## Quick reference

```bash
# controller (one machine, keep running)
java -jar linkmesh.jar controller --port 9000 --replication 2 --reducers 2

# add a machine (any number, any time)
java -jar linkmesh.jar worker --controller HOST:9000 [--role reducer|mapper|auto]

# load data (once)
java -jar linkmesh.jar ingest --controller HOST:9000 --warc FILE --partitions 64 --maxPages 100000

# run a job (repeatable)
java -jar linkmesh.jar submit --controller HOST:9000 --job NAME

# inspect / cancel
java -jar linkmesh.jar status --controller HOST:9000
java -jar linkmesh.jar cancel --controller HOST:9000
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for how it works internally, and the
[README](../README.md) for the demo scripts.

---

## Appendix: multi-machine benchmarking on GitHub Actions

Every benchmark in the README runs on one machine, which can never show whether
adding machines helps. Splitting one host into more node processes only costs.

GitHub Actions gives each job in a matrix its own VM: 4 cores, 15 GB, free and
unmetered on public repos. Runners have no network route to each other, so they
are joined into a Tailscale network to make a real cluster out of them.

### One-time setup

1. Create a free account at [tailscale.com](https://tailscale.com). You do **not**
   need to install Tailscale on your own computer; the runners are the devices.

2. Admin console, Access controls. Add a tag the CI nodes can claim:

   ```json
   "tagOwners": {
     "tag:ci": ["autogroup:admin"]
   }
   ```

3. Admin console, Settings, Keys, Generate auth key:
   - **Reusable**: on (several runners use the same key)
   - **Ephemeral**: on (nodes remove themselves when the job ends)
   - **Tags**: `tag:ci`

   Copy the key. It is shown once.

4. Store it as a repository secret, without pasting it anywhere else:

   ```bash
   gh secret set TS_AUTHKEY --repo <owner>/<repo>
   ```

### Running it

```bash
gh workflow run multi-machine.yml -f workers=1 -f dataset=web-BerkStan
gh workflow run multi-machine.yml -f workers=2 -f dataset=web-BerkStan
gh workflow run multi-machine.yml -f workers=4 -f dataset=web-BerkStan
```

Each run reports its best map-stage time in the job summary. Comparing the three
gives the curve that a single host cannot produce.

### How the jobs find each other

The controller and the workers start at the same time, with no `needs:` between
them, because a dependent job only starts after the other has finished. Instead
each joins the tailnet under a predictable MagicDNS name derived from the run id,
and workers dial `lm-controller-<run_id>:9000`. A worker retries the connection
indefinitely, so it does not matter which job is ready first.

One detail worth knowing if you adapt this: workers must pass
`--advertise $(tailscale ip -4)`. Automatic address detection picks the runner's
own private address, which no other runner can route to. The tailnet address is
the only one that works.
