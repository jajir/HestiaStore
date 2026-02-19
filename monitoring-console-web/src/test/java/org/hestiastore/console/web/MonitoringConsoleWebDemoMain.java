package org.hestiastore.console.web;

import java.io.IOException;
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
 * One-command local demo for direct monitoring-console-web mode.
 */
public final class MonitoringConsoleWebDemoMain {

    private static final int DEFAULT_BASE_PORT = 9001;
    private static final int DEMO_MAX_KEYS_PER_SEGMENT = 128;
    private static final int DEMO_MAX_KEYS_IN_SEGMENT_CACHE = 64;
    private static final int DEMO_MAX_SEGMENTS_IN_CACHE = 16;
    private static final int DEMO_WRITE_CACHE_KEYS = 8;
    private static final int DEMO_WRITE_CACHE_KEYS_DURING_MAINTENANCE = 16;
    private static final int DEMO_MAX_DELTA_CACHE_FILES = 2;
    private static final int DEMO_BLOOM_INDEX_SIZE_BYTES = 256 * 1024;
    private static final int DEMO_KEY_SPACE = 10_000;
    private static final int NODE_COUNT = 3;

    private MonitoringConsoleWebDemoMain() {
    }

    /**
     * Starts 3 demo nodes with management-agent and generates periodic traffic.
     *
     * @param args first arg optionally sets base port (default 9001)
     * @throws Exception on startup failure
     */
    public static void main(final String[] args) throws Exception {
        final int basePort = resolveBasePort(args);
        final List<SegmentIndex<Integer, String>> indexes = new ArrayList<>();
        final List<ManagementAgentServer> agents = new ArrayList<>();
        final ScheduledExecutorService loadExecutor = Executors
                .newSingleThreadScheduledExecutor();
        final List<List<String>> nodeIndexNames = List.of(
                List.of("index-1"),
                List.of("index-2-a", "index-2-b"),
                List.of("index-3-a", "index-3-b"));

        for (int i = 0; i < NODE_COUNT; i++) {
            final List<String> namesForNode = nodeIndexNames.get(i);
            final List<SegmentIndex<Integer, String>> indexesForNode = new ArrayList<>();
            for (final String indexName : namesForNode) {
                final SegmentIndex<Integer, String> index = createIndex(indexName);
                indexesForNode.add(index);
                indexes.add(index);
            }
            final ManagementAgentServer agent = new ManagementAgentServer(
                    "127.0.0.1", basePort + i, indexesForNode.get(0),
                    namesForNode.get(0),
                    Set.of("indexBusyTimeoutMillis"));
            for (int j = 1; j < indexesForNode.size(); j++) {
                agent.addIndex(namesForNode.get(j), indexesForNode.get(j));
            }
            agents.add(agent);
            agent.start();
        }

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
        }));

        System.out.println("Management agents started (direct mode):");
        for (int i = 0; i < NODE_COUNT; i++) {
            System.out.println("  node-" + (i + 1) + ": http://127.0.0.1:"
                    + (basePort + i) + " indexes="
                    + String.join(",", nodeIndexNames.get(i)));
        }
        Thread.currentThread().join();
    }

    private static int resolveBasePort(final String[] args) {
        if (args != null && args.length > 0 && args[0] != null
                && !args[0].isBlank()) {
            try {
                return Integer.parseInt(args[0].trim());
            } catch (final NumberFormatException ignored) {
                // fallback below
            }
        }
        return Integer.getInteger("hestia.console.web.demo.basePort",
                DEFAULT_BASE_PORT);
    }

    private static SegmentIndex<Integer, String> createIndex(final String name)
            throws IOException {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withMaxNumberOfKeysInSegmentCache(
                        DEMO_MAX_KEYS_IN_SEGMENT_CACHE)
                .withMaxNumberOfSegmentsInCache(DEMO_MAX_SEGMENTS_IN_CACHE)
                .withMaxNumberOfKeysInSegment(DEMO_MAX_KEYS_PER_SEGMENT)
                .withMaxNumberOfKeysInSegmentWriteCache(DEMO_WRITE_CACHE_KEYS)
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(
                        DEMO_WRITE_CACHE_KEYS_DURING_MAINTENANCE)
                .withMaxNumberOfDeltaCacheFiles(DEMO_MAX_DELTA_CACHE_FILES)
                .withBloomFilterIndexSizeInBytes(DEMO_BLOOM_INDEX_SIZE_BYTES)
                .withContextLoggingEnabled(false)
                .withName(name)
                .build();
        return SegmentIndex.create(directory, conf);
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
