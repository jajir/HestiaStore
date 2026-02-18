package org.hestiastore.console;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.management.agent.ManagementAgentServer;

/**
 * One-command local demo for monitoring console and node agents.
 */
public final class MonitoringConsoleDemoMain {

    private static final String TOKEN = "demo-token";
    private static final int DEFAULT_CONSOLE_PORT = 8085;
    private static final int DEMO_MAX_KEYS_PER_SEGMENT = 128;
    private static final int DEMO_MAX_KEYS_IN_SEGMENT_CACHE = 64;
    private static final int DEMO_MAX_SEGMENTS_IN_CACHE = 16;
    private static final int DEMO_WRITE_CACHE_KEYS = 8;
    private static final int DEMO_WRITE_CACHE_KEYS_DURING_MAINTENANCE = 16;
    private static final int DEMO_MAX_DELTA_CACHE_FILES = 2;
    private static final int DEMO_BLOOM_INDEX_SIZE_BYTES = 256 * 1024;
    private static final int DEMO_KEY_SPACE = 10_000;

    private MonitoringConsoleDemoMain() {
    }

    /**
     * Starts 3 demo nodes + console and generates periodic traffic.
     *
     * @param args ignored
     * @throws Exception on startup failure
     */
    public static void main(final String[] args) throws Exception {
        final int consolePort = resolvePort(args);
        final List<SegmentIndex<Integer, String>> indexes = new ArrayList<>();
        final List<ManagementAgentServer> agents = new ArrayList<>();
        final ScheduledExecutorService loadExecutor = Executors
                .newSingleThreadScheduledExecutor();

        final MonitoringConsoleServer console = new MonitoringConsoleServer(
                "127.0.0.1", consolePort, TOKEN, false, 3);
        console.start();

        for (int i = 1; i <= 3; i++) {
            final SegmentIndex<Integer, String> index = createIndex("index-" + i);
            indexes.add(index);
            final ManagementAgentServer agent = new ManagementAgentServer(
                    "127.0.0.1", 0, index, "index-" + i,
                    Set.of("indexBusyTimeoutMillis"));
            agents.add(agent);
            agent.start();
        }

        registerAllNodes(console.getPort(), agents);
        startSyntheticLoad(loadExecutor, indexes);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            loadExecutor.shutdownNow();
            for (final ManagementAgentServer agent : agents) {
                agent.close();
            }
            for (final SegmentIndex<Integer, String> index : indexes) {
                if (!index.wasClosed()) {
                    index.close();
                }
            }
            console.close();
        }));

        System.out.println("Monitoring console started.");
        System.out.println("Dashboard API:   http://127.0.0.1:" + consolePort
                + "/console/v1/dashboard");
        System.out.println("Nodes API:       http://127.0.0.1:" + consolePort
                + "/console/v1/nodes");
        System.out.println("Use UI from monitoring-console-web module.");
        System.out.println("Write token:     " + TOKEN);
        Thread.currentThread().join();
    }

    private static int resolvePort(final String[] args) {
        if (args != null && args.length > 0 && args[0] != null
                && !args[0].isBlank()) {
            try {
                return Integer.parseInt(args[0].trim());
            } catch (final NumberFormatException ignored) {
                // fallback below
            }
        }
        return Integer.getInteger("hestia.console.demo.port",
                DEFAULT_CONSOLE_PORT);
    }

    private static SegmentIndex<Integer, String> createIndex(final String name)
            throws IOException {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withMaxNumberOfKeysInSegmentCache(
                        DEMO_MAX_KEYS_IN_SEGMENT_CACHE)//
                .withMaxNumberOfSegmentsInCache(DEMO_MAX_SEGMENTS_IN_CACHE)//
                .withMaxNumberOfKeysInSegment(DEMO_MAX_KEYS_PER_SEGMENT)//
                .withMaxNumberOfKeysInSegmentWriteCache(DEMO_WRITE_CACHE_KEYS)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(
                        DEMO_WRITE_CACHE_KEYS_DURING_MAINTENANCE)//
                .withMaxNumberOfDeltaCacheFiles(DEMO_MAX_DELTA_CACHE_FILES)//
                .withBloomFilterIndexSizeInBytes(DEMO_BLOOM_INDEX_SIZE_BYTES)//
                .withContextLoggingEnabled(false)//
                .withName(name)//
                .build();
        return SegmentIndex.create(directory, conf);
    }

    private static void registerAllNodes(final int consolePort,
            final List<ManagementAgentServer> agents)
            throws IOException, InterruptedException {
        final HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2)).build();
        for (int i = 0; i < agents.size(); i++) {
            final String nodeId = "node-" + (i + 1);
            final String nodeName = "index-" + (i + 1);
            final String baseUrl = "http://127.0.0.1:" + agents.get(i).getPort();
            final String payload = """
                    {"nodeId":"%s","nodeName":"%s","baseUrl":"%s","agentToken":""}
                    """.formatted(nodeId, nodeName, baseUrl);
            final HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(
                            "http://127.0.0.1:" + consolePort + "/console/v1/nodes"))
                    .header("Content-Type", "application/json")
                    .header("X-Hestia-Console-Token", TOKEN)
                    .POST(HttpRequest.BodyPublishers.ofString(payload)).build();
            client.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }

    private static void startSyntheticLoad(
            final ScheduledExecutorService loadExecutor,
            final List<SegmentIndex<Integer, String>> indexes) {
        final Random random = new Random();
        loadExecutor.scheduleAtFixedRate(() -> {
            try {
                for (final SegmentIndex<Integer, String> index : indexes) {
                    final int key = random.nextInt(DEMO_KEY_SPACE);
                    final int coldMissKey = key + 10_000_000;
                    final int coldMissKey2 = key + 20_000_000;
                    final int coldMissKey3 = key + 30_000_000;
                    index.put(key, "v" + random.nextInt(1000));
                    index.get(key);
                    // Force cache-miss lookups so Bloom filter counters move.
                    index.get(coldMissKey);
                    index.get(coldMissKey2);
                    index.get(coldMissKey3);
                    if (random.nextInt(20) == 0) {
                        index.delete(key);
                    }
                }
            } catch (final Exception ignored) {
                // best-effort demo load generator
            }
        }, 0L, 250L, TimeUnit.MILLISECONDS);
    }
}
