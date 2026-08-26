package linkmesh;

import linkmesh.cluster.NodeRole;
import linkmesh.common.Args;
import linkmesh.common.Log;
import linkmesh.common.Text;
import linkmesh.controller.Controller;
import linkmesh.ingest.Ingestor;
import linkmesh.ingest.SyntheticCorpus;
import linkmesh.proto.ConnectionPool;
import linkmesh.proto.Endpoint;
import linkmesh.proto.Message;
import linkmesh.proto.Nets;
import linkmesh.proto.Verbs;
import linkmesh.worker.Worker;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Single entry point for every role.
 *
 * The whole cluster ships as one jar. Copy it to a machine, point it at the
 * controller, and it joins -- identity, listen address, and data directory are
 * all derived, so the common case needs exactly one flag.
 */
public final class Main {

    private static final int DEFAULT_CONTROLLER_PORT = 9000;
    private static final int DEFAULT_WORKER_PORT = 7100;

    public static void main(String[] argv) {
        if (argv.length == 0 || argv[0].equals("help") || argv[0].equals("--help") || argv[0].equals("-h")) {
            usage();
            System.exit(argv.length == 0 ? 1 : 0);
        }
        String command = argv[0];
        String[] rest = new String[argv.length - 1];
        System.arraycopy(argv, 1, rest, 0, rest.length);
        Args args = new Args(rest);

        if (args.getBool("verbose", false)) Log.setLevel(Log.Level.DEBUG);
        if (args.getBool("quiet", false)) Log.setLevel(Log.Level.WARN);

        try {
            switch (command) {
                case "controller" -> runController(args);
                case "worker" -> runWorker(args);
                case "ingest" -> runIngest(args);
                case "gen" -> runGenerate(args);
                case "submit", "run" -> runSubmit(args);
                case "status" -> runStatus(args);
                case "cancel" -> runCancel(args);
                default -> {
                    System.err.println("unknown command: " + command);
                    usage();
                    System.exit(1);
                }
            }
        } catch (IllegalArgumentException | IllegalStateException e) {
            System.err.println("error: " + e.getMessage());
            System.exit(2);
        } catch (Exception e) {
            System.err.println("error: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            if (Log.level() == Log.Level.DEBUG) e.printStackTrace();
            System.exit(3);
        }
    }

    // ------------------------------------------------------------- controller

    private static void runController(Args args) throws Exception {
        Controller.Config config = new Controller.Config();
        config.port = args.getInt("port", DEFAULT_CONTROLLER_PORT);
        config.replicationFactor = args.getInt("replication", 2);
        config.reducerCount = args.getInt("reducers", 2);
        config.outputPath = Path.of(args.get("output", "output/backlinks.tsv"));
        config.suspectMillis = args.getLong("suspectMs", 3000);
        config.deadMillis = args.getLong("deadMs", 8000);
        config.minWorkers = args.getInt("minWorkers", 1);

        config.tuning.speculativeMultiplier = args.getDouble("speculativeMultiplier", 1.5);
        config.tuning.minSpeculativeMillis = args.getLong("minSpeculativeMs", 5000);
        config.tuning.allowRemoteFetch = args.getBool("remoteFetch", true);
        config.tuning.parserThreads = args.getInt("parserThreads", 0);
        config.tuning.queueCapacity = args.getInt("queueCapacity", 0);
        config.tuning.shuffleBatch = args.getInt("shuffleBatch", 0);
        config.tuning.parseDelayMillis = args.getInt("parseDelayMs", 0);
        if (args.getBool("noSpeculation", false)) {
            config.tuning.maxSpeculativeAttempts = 0;
        }

        Controller controller = new Controller(config);
        Runtime.getRuntime().addShutdownHook(new Thread(controller::close));
        int port = controller.start();
        System.out.println();
        System.out.println("  LinkMesh controller ready on " + Nets.detectAdvertiseAddress() + ":" + port);
        System.out.println("  Join a node with:  java -jar linkmesh.jar worker --controller "
                + Nets.detectAdvertiseAddress() + ":" + port);
        System.out.println();

        if (args.has("job") || args.getBool("autoStart", false)) {
            String jobId = args.get("job", "job-" + System.currentTimeMillis());
            int waitFor = args.getInt("waitForNodes", config.minWorkers);
            waitForNodes(controller, waitFor, args.getLong("waitMs", 60_000));
            controller.startJob(jobId);
            controller.awaitJob(args.getLong("jobTimeoutMs", 3_600_000), TimeUnit.MILLISECONDS);
            controller.close();
            return;
        }
        Thread.currentThread().join();
    }

    private static void waitForNodes(Controller controller, int wanted, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (controller.cluster().aliveCount() < wanted) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("timed out waiting for " + wanted + " nodes, have "
                        + controller.cluster().aliveCount());
            }
            Thread.sleep(250);
        }
        // Give the first inventory refresh a chance to land before scheduling.
        Thread.sleep(1500);
    }

    // ----------------------------------------------------------------- worker

    private static void runWorker(Args args) throws Exception {
        Worker.Config config = new Worker.Config();
        config.controller = Endpoint.parse(args.require("controller"), DEFAULT_CONTROLLER_PORT);
        config.port = args.getInt("port", DEFAULT_WORKER_PORT);
        config.advertiseHost = args.get("advertise", null);

        String hostname = sanitize(localHostname());
        config.nodeId = args.get("id", config.port == DEFAULT_WORKER_PORT ? hostname : hostname + "-" + config.port);
        config.dataDir = Path.of(args.get("data", "linkmesh-data/" + config.nodeId));

        config.role = NodeRole.parse(args.get("role", "auto"));
        config.slots = args.getInt("slots", 1);
        config.parserThreads = args.getInt("parserThreads", config.parserThreads);
        config.queueCapacity = args.getInt("queueCapacity", config.queueCapacity);
        config.shuffleBatch = args.getInt("shuffleBatch", config.shuffleBatch);
        config.parseDelayMillis = args.getInt("parseDelayMs", 0);
        config.suspectMillis = args.getLong("suspectMs", 3000);
        config.deadMillis = args.getLong("deadMs", 8000);
        config.gossipFanout = args.getInt("gossipFanout", 2);

        Worker worker = Worker.start(config);
        Runtime.getRuntime().addShutdownHook(new Thread(worker::close));
        System.out.println();
        System.out.println("  LinkMesh node " + worker.nodeId() + " ready on " + worker.endpoint());
        System.out.println("  Data directory: " + worker.store().root());
        System.out.println();
        Thread.currentThread().join();
    }

    // ----------------------------------------------------------------- ingest

    private static void runIngest(Args args) throws Exception {
        Ingestor.Config config = new Ingestor.Config();
        // --out parses to local disk and never talks to a cluster, so the
        // controller address is only required for the paths that push.
        if (!args.has("out")) {
            config.controller = Endpoint.parse(args.require("controller"), DEFAULT_CONTROLLER_PORT);
        }
        config.partitions = args.getInt("partitions", 32);
        config.pagesPerFile = args.getInt("pagesPerFile", 200);
        config.externalOnly = !args.getBool("includeInternal", false);
        config.maxPages = args.getLong("maxPages", Long.MAX_VALUE);
        config.staging = Path.of(args.get("staging", "build/ingest-staging"));

        // --out parses the corpus into partition directories and stops there,
        // so the same corpus can be reloaded many times without re-parsing it.
        boolean extractOnly = args.has("out");
        if (extractOnly) config.staging = Path.of(args.require("out"));

        Ingestor ingestor = new Ingestor(config);
        try {
            Ingestor.Stats stats;
            if (extractOnly) {
                Ingestor.Format format;
                String spec;
                if (args.has("wikipedia")) {
                    format = Ingestor.Format.WIKIPEDIA;
                    spec = args.require("wikipedia");
                } else if (args.has("edges")) {
                    format = Ingestor.Format.EDGES;
                    spec = args.require("edges");
                } else {
                    format = Ingestor.Format.WARC;
                    spec = args.require("warc");
                }
                List<Path> inputs = resolveInputs(spec, ".tar.gz", ".tgz", ".ndjson",
                        ".warc", ".warc.gz", ".txt", ".txt.gz", ".edges");
                if (inputs.isEmpty()) throw new IllegalArgumentException("no input files found");
                System.out.println("Extracting " + inputs.size() + " file(s) to " + config.staging);
                stats = ingestor.extractOnly(inputs, format);
                printIngestSummary(stats);
            } else if (args.has("prepared")) {
                Path prepared = Path.of(args.require("prepared"));
                if (!Files.isDirectory(prepared)) throw new IllegalArgumentException("not a directory: " + prepared);
                stats = ingestor.ingestPrepared(prepared);
                System.out.printf("Pushed %d prepared partitions (%s) in %s%n",
                        stats.partitionsPushed(), Text.humanBytes(stats.bytesPushed()),
                        Text.humanMillis(stats.elapsedMillis()));
            } else if (args.has("edges")) {
                List<Path> inputs = resolveInputs(args.require("edges"), ".txt", ".txt.gz", ".edges", ".gz");
                if (inputs.isEmpty()) throw new IllegalArgumentException("no edge list files found");
                System.out.println("Ingesting " + inputs.size() + " edge list file(s)");
                stats = ingestor.ingestEdges(inputs);
                printIngestSummary(stats);
            } else if (args.has("wikipedia")) {
                List<Path> inputs = resolveInputs(args.require("wikipedia"), ".tar.gz", ".tgz", ".ndjson");
                if (inputs.isEmpty()) throw new IllegalArgumentException("no Wikipedia dump files found");
                System.out.println("Ingesting " + inputs.size() + " Wikipedia dump file(s)");
                stats = ingestor.ingestWikipedia(inputs);
                printIngestSummary(stats);
            } else {
                List<Path> inputs = resolveInputs(args.require("warc"), ".warc", ".warc.gz");
                if (inputs.isEmpty()) throw new IllegalArgumentException("no WARC files found");
                System.out.println("Ingesting " + inputs.size() + " file(s), external links only: "
                        + config.externalOnly);
                stats = ingestor.ingest(inputs);
                printIngestSummary(stats);
            }
        } finally {
            ingestor.close();
        }
    }

    private static void printIngestSummary(Ingestor.Stats stats) {
                System.out.println();
                System.out.printf("  records scanned : %,d%n", stats.recordsSeen());
                System.out.printf("  pages indexed   : %,d%n", stats.pagesWritten());
                System.out.printf("  links extracted : %,d%n", stats.linksWritten());
                System.out.printf("  partitions      : %d (%s pushed including replicas)%n",
                        stats.partitionsPushed(), Text.humanBytes(stats.bytesPushed()));
                System.out.printf("  elapsed         : %s%n", Text.humanMillis(stats.elapsedMillis()));
                System.out.println();
    }

    private static List<Path> resolveInputs(String spec, String... suffixes) throws IOException {
        Path path = Path.of(spec);
        if (Files.isRegularFile(path)) return List.of(path);
        if (!Files.isDirectory(path)) throw new IllegalArgumentException("no such file or directory: " + spec);
        List<Path> files = new ArrayList<>();
        try (var stream = Files.list(path)) {
            for (Path child : stream.sorted().toList()) {
                String name = child.getFileName().toString().toLowerCase();
                for (String suffix : suffixes) {
                    if (name.endsWith(suffix)) { files.add(child); break; }
                }
            }
        }
        return files;
    }

    // -------------------------------------------------------------- generator

    private static void runGenerate(Args args) throws Exception {
        Path out = Path.of(args.get("out", "build/synthetic"));
        int pages = args.getInt("pages", 20_000);
        int partitions = args.getInt("partitions", 16);
        int links = args.getInt("links", 10);
        int pagesPerFile = args.getInt("pagesPerFile", 200);
        long seed = args.getLong("seed", 42);
        double skew = args.getDouble("skew", 1.0);

        linkmesh.storage.LocalStore.deleteRecursively(out);
        SyntheticCorpus.Stats stats = SyntheticCorpus.generate(out, pages, partitions, links, pagesPerFile, seed, skew);
        System.out.printf("Generated %,d pages and %,d links in %d partitions at %s%n",
                stats.pages(), stats.links(), stats.partitions(), out.toAbsolutePath());
        System.out.println("Push it into a running cluster with:");
        System.out.println("  java -jar linkmesh.jar ingest --controller HOST:9000 --prepared " + out);
    }

    // ------------------------------------------------------------ job control

    private static void runSubmit(Args args) throws Exception {
        Endpoint controller = Endpoint.parse(args.require("controller"), DEFAULT_CONTROLLER_PORT);
        String jobId = args.get("job", "job-" + System.currentTimeMillis());
        try (ConnectionPool pool = new ConnectionPool(5000, 30_000)) {
            Message reply = pool.request(controller, Message.of(Verbs.SUBMIT, "job", jobId));
            if (reply.isError()) throw new IllegalStateException(reply.get("reason", "submit rejected"));
            System.out.printf("Submitted job %s over %s partitions.%n", jobId, reply.get("partitions", "?"));
            if (!args.getBool("wait", true)) return;

            System.out.println("Waiting for completion (Ctrl-C to detach; the job keeps running)...");
            String lastProgress = "";
            while (true) {
                Thread.sleep(1000);
                Message status = pool.request(controller, Message.of(Verbs.STATUS));
                String body = status.bodyText();
                String phase = valueOf(body, "phase");
                String progress = valueOf(body, "progress");
                if (progress != null && !progress.equals(lastProgress)) {
                    System.out.println("  " + progress + " partitions complete");
                    lastProgress = progress;
                }
                if ("DONE".equals(phase)) { System.out.println("Job complete."); return; }
                if ("FAILED".equals(phase)) { System.out.println("Job failed; see controller log."); return; }
            }
        }
    }

    private static void runStatus(Args args) throws Exception {
        Endpoint controller = Endpoint.parse(args.require("controller"), DEFAULT_CONTROLLER_PORT);
        try (ConnectionPool pool = new ConnectionPool(5000, 30_000)) {
            Message reply = pool.request(controller, Message.of(Verbs.STATUS));
            System.out.println(reply.bodyText());
        }
    }

    private static void runCancel(Args args) throws Exception {
        Endpoint controller = Endpoint.parse(args.require("controller"), DEFAULT_CONTROLLER_PORT);
        try (ConnectionPool pool = new ConnectionPool(5000, 30_000)) {
            Message request = Message.of(Verbs.CANCEL_JOB);
            if (args.has("job")) request = request.with("job", args.require("job"));
            pool.request(controller, request);
            System.out.println("Cancellation sent.");
        }
    }

    private static String valueOf(String body, String key) {
        for (String line : body.split("\n")) {
            if (line.startsWith(key + "=")) return line.substring(key.length() + 1).trim();
        }
        return null;
    }

    private static String localHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "node";
        }
    }

    private static String sanitize(String value) {
        return value.replaceAll("[^A-Za-z0-9_-]", "-").toLowerCase();
    }

    private static void usage() {
        System.out.println("""
                LinkMesh - distributed backlink indexer

                  controller   Run the cluster controller
                    --port 9000            listen port
                    --replication 2        replicas per partition
                    --reducers 2           reducer count
                    --output PATH          output TSV (default output/backlinks.tsv)
                    --job NAME             run one job as soon as nodes join, then exit
                    --waitForNodes N       with --job, wait for N nodes first
                    --noSpeculation        disable straggler backup attempts
                    --parseDelayMs N       artificial per-page delay, for benchmarking

                  worker       Run a cluster node (storage + compute)
                    --controller HOST:PORT required
                    --id NAME              node id (default: hostname)
                    --port 7100            listen port
                    --data DIR             replica store (default: linkmesh-data/<id>)
                    --advertise HOST       override detected LAN address
                    --slots N              concurrent map tasks (default: cores/2)
                                           the main throughput knob on one machine
                    --parserThreads 2      threads inside one map task
                    --role auto            auto | reducer | mapper
                                           reducer: prefer this machine to hold reducer state
                                           mapper:  never use it as a reducer

                  ingest       Load a corpus into the cluster
                    --controller HOST:PORT required
                    --warc FILE|DIR        WARC or WARC.GZ input
                    --wikipedia FILE|DIR   Wikipedia Enterprise HTML dump (.tar.gz)
                    --edges FILE|DIR       plain edge list, "src dst" per line (.txt/.txt.gz)
                    --prepared DIR         push an already-partitioned directory
                    --out DIR              parse to DIR and stop (no cluster needed)
                    --partitions 32        number of partitions to create
                    --maxPages N           stop after N pages
                    --includeInternal      keep same-site links (off by default)

                  gen          Generate a synthetic corpus locally
                    --out DIR --pages N --partitions P [--links 10] [--skew 1.0]

                  submit       Start a job and follow progress
                    --controller HOST:PORT [--job NAME] [--wait false]

                  status       Print cluster and job status
                  cancel       Cancel the running job

                Every flag also reads from LINKMESH_<FLAG> in the environment.
                """);
    }
}
