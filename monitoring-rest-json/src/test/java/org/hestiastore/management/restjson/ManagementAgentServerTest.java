package org.hestiastore.management.restjson;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.monitoring.json.api.ActionResponse;
import org.hestiastore.monitoring.json.api.ActionStatus;
import org.hestiastore.monitoring.json.api.ActionType;
import org.hestiastore.monitoring.json.api.ConfigViewResponse;
import org.hestiastore.monitoring.json.api.ErrorResponse;
import org.hestiastore.monitoring.json.api.ManagementApiPaths;
import org.hestiastore.monitoring.json.api.NodeReportResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

class ManagementAgentServerTest {

    private static final String INDEX_1 = "agent-test-index-1";
    private static final String INDEX_2 = "agent-test-index-2";

    private final List<SegmentIndex<Integer, String>> indexes = new ArrayList<>();
    private ManagementAgentServer server;
    private HttpClient client;
    private ObjectMapper objectMapper;
    private String baseUrl;

    @BeforeEach
    void setUp() throws IOException {
        final SegmentIndex<Integer, String> index1 = createIndex(INDEX_1);
        final SegmentIndex<Integer, String> index2 = createIndex(INDEX_2);
        indexes.add(index1);
        indexes.add(index2);

        server = new ManagementAgentServer("127.0.0.1", 0, index1, INDEX_1);
        server.addIndex(INDEX_2, index2);
        server.start();

        client = HttpClient.newHttpClient();
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        baseUrl = "http://127.0.0.1:" + server.getPort();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
        for (final SegmentIndex<Integer, String> index : indexes) {
            if (index != null && !index.wasClosed()) {
                index.close();
            }
        }
    }

    @Test
    void reportEndpointReturnsJvmAndPerIndexSections() throws Exception {
        indexes.get(0).put(1, "A");
        indexes.get(1).put(2, "B");

        final HttpResponse<String> reportResp = send("GET",
                ManagementApiPaths.REPORT, null);
        assertEquals(200, reportResp.statusCode());

        final JsonNode reportJson = objectMapper.readTree(reportResp.body());
        final NodeReportResponse report = objectMapper
                .readValue(reportResp.body(), NodeReportResponse.class);
        assertEquals(2, report.indexes().size());
        assertTrue(report.indexes().stream()
                .anyMatch(idx -> idx.indexName().equals(INDEX_1)));
        assertTrue(report.indexes().stream()
                .anyMatch(idx -> idx.indexName().equals(INDEX_2)));
        assertTrue(report.jvm().heapUsedBytes() >= 0L);
        assertTrue(report.jvm().heapMaxBytes() >= 0L);
        assertTrue(report.jvm().gcCount() >= 0L);
        assertTrue(report.indexes().stream()
                .allMatch(idx -> idx.segmentRuntimeSnapshots() != null));
        assertTrue(report.indexes().stream()
                .allMatch(idx -> idx.partitionCount() >= 0));
        for (final JsonNode indexNode : reportJson.path("indexes")) {
            assertTrue(indexNode.has("maxNumberOfKeysInActivePartition"));
            assertTrue(indexNode.has("maxNumberOfImmutableRunsPerPartition"));
            assertTrue(indexNode.has("maxNumberOfKeysInPartitionBuffer"));
            assertTrue(indexNode.has("maxNumberOfKeysInIndexBuffer"));
            assertTrue(!indexNode.has("maxNumberOfKeysInSegmentWriteCache"));
            assertTrue(!indexNode.has(
                    "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance"));
            assertTrue(indexNode.has("drainLatencyP95Micros"));
        }
    }

    @Test
    void flushActionRunsOnAllIndexes() throws Exception {
        indexes.get(0).put(1, "A");
        indexes.get(1).put(2, "B");

        final HttpResponse<String> response = send("POST",
                ManagementApiPaths.ACTION_FLUSH, "{\"requestId\":\"req-1\"}");
        assertEquals(200, response.statusCode());

        final ActionResponse payload = objectMapper.readValue(response.body(),
                ActionResponse.class);
        assertEquals("req-1", payload.requestId());
        assertEquals(ActionType.FLUSH, payload.action());
        assertEquals(ActionStatus.COMPLETED, payload.status());
        assertTrue(payload.message().contains("2"));
    }

    @Test
    void compactActionCanTargetOneIndexByName() throws Exception {
        indexes.get(0).put(1, "A");
        indexes.get(1).put(2, "B");

        final HttpResponse<String> response = send("POST",
                ManagementApiPaths.ACTION_COMPACT,
                "{\"requestId\":\"req-compact\",\"indexName\":\"" + INDEX_2
                        + "\"}");
        assertEquals(200, response.statusCode());

        final ActionResponse payload = objectMapper.readValue(response.body(),
                ActionResponse.class);
        assertEquals("req-compact", payload.requestId());
        assertEquals(ActionType.COMPACT, payload.action());
        assertEquals(ActionStatus.COMPLETED, payload.status());
        assertTrue(payload.message().contains("1"));
    }

    @Test
    void duplicateRequestIdReplaysCompletedActionWithoutRunningAgain()
            throws Exception {
        final AtomicInteger flushCalls = new AtomicInteger();
        server.addIndex(INDEX_1, instrumentAction(indexes.get(0),
                "flushAndWait", flushCalls, null));

        final String requestBody = "{\"requestId\":\"req-replay\",\"indexName\":\""
                + INDEX_1 + "\"}";
        final HttpResponse<String> first = send("POST",
                ManagementApiPaths.ACTION_FLUSH, requestBody);
        final HttpResponse<String> second = send("POST",
                ManagementApiPaths.ACTION_FLUSH, requestBody);

        assertEquals(200, first.statusCode());
        assertEquals(200, second.statusCode());
        assertEquals(first.body(), second.body());
        assertEquals(1, flushCalls.get());
    }

    @Test
    void oldestReplayEntryIsEvictedWhenReplayRetentionLimitIsReached()
            throws Exception {
        server.close();
        final AtomicInteger flushCalls = new AtomicInteger();
        server = new ManagementAgentServer("127.0.0.1", 0,
                ManagementAgentSecurityPolicy.permissive(), 2);
        server.addIndex(INDEX_1, instrumentAction(indexes.get(0),
                "flushAndWait", flushCalls, null));
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getPort();

        assertEquals(200, send("POST", ManagementApiPaths.ACTION_FLUSH,
                "{\"requestId\":\"req-1\",\"indexName\":\"" + INDEX_1 + "\"}")
                .statusCode());
        assertEquals(200, send("POST", ManagementApiPaths.ACTION_FLUSH,
                "{\"requestId\":\"req-2\",\"indexName\":\"" + INDEX_1 + "\"}")
                .statusCode());
        assertEquals(200, send("POST", ManagementApiPaths.ACTION_FLUSH,
                "{\"requestId\":\"req-3\",\"indexName\":\"" + INDEX_1 + "\"}")
                .statusCode());
        assertEquals(200, send("POST", ManagementApiPaths.ACTION_FLUSH,
                "{\"requestId\":\"req-1\",\"indexName\":\"" + INDEX_1 + "\"}")
                .statusCode());
        assertEquals(4, flushCalls.get());
    }

    @Test
    void bulkActionFailureReportsPartialProgressAndReplaysByRequestId()
            throws Exception {
        final AtomicInteger successfulFlushCalls = new AtomicInteger();
        final AtomicInteger failingFlushCalls = new AtomicInteger();
        server.addIndex(INDEX_1, instrumentAction(indexes.get(0),
                "flushAndWait", successfulFlushCalls, null));
        server.addIndex(INDEX_2, instrumentAction(indexes.get(1),
                "flushAndWait", failingFlushCalls,
                new IllegalStateException("simulated failure")));

        final String requestBody = "{\"requestId\":\"req-partial\"}";
        final HttpResponse<String> first = send("POST",
                ManagementApiPaths.ACTION_FLUSH, requestBody);
        final HttpResponse<String> second = send("POST",
                ManagementApiPaths.ACTION_FLUSH, requestBody);

        assertEquals(500, first.statusCode());
        assertEquals(500, second.statusCode());
        assertEquals(first.body(), second.body());

        final ActionResponse payload = objectMapper.readValue(first.body(),
                ActionResponse.class);
        assertEquals(ActionStatus.FAILED, payload.status());
        assertTrue(payload.message()
                .contains("Applied to 1 of 2 index(es) before failure"));
        assertTrue(payload.message().contains(INDEX_2));
        assertEquals(1, successfulFlushCalls.get());
        assertEquals(1, failingFlushCalls.get());
    }

    @Test
    void targetedActionReturnsNotFoundForUnknownIndex() throws Exception {
        final HttpResponse<String> response = send("POST",
                ManagementApiPaths.ACTION_FLUSH,
                "{\"requestId\":\"req-unknown\",\"indexName\":\"missing\"}");
        assertEquals(404, response.statusCode());
        final ErrorResponse payload = objectMapper.readValue(response.body(),
                ErrorResponse.class);
        assertEquals("INDEX_NOT_FOUND", payload.code());
        assertEquals("req-unknown", payload.requestId());
    }

    @Test
    void configPatchRejectsInvalidRequest() throws Exception {
        for (final String[] invalidRequest : invalidConfigPatchRequests()) {
            final HttpResponse<String> response = send("PATCH",
                    invalidRequest[0], invalidRequest[1]);
            assertEquals(400, response.statusCode());
            final ErrorResponse payload = objectMapper
                    .readValue(response.body(), ErrorResponse.class);
            assertEquals(invalidRequest[2], payload.code());
        }
    }

    @Test
    void configGetReturnsRuntimeConfigViewOnly() throws Exception {
        final HttpResponse<String> response = send("GET",
                ManagementApiPaths.CONFIG + "?indexName=" + INDEX_1, null);
        assertEquals(200, response.statusCode());
        final ConfigViewResponse payload = objectMapper
                .readValue(response.body(), ConfigViewResponse.class);
        assertEquals(INDEX_1, payload.indexName());
        assertTrue(
                payload.original().containsKey("maxNumberOfSegmentsInCache"));
        assertTrue(
                payload.current().containsKey("maxNumberOfKeysInSegmentCache"));
        assertTrue(
                payload.supportedKeys().contains("maxNumberOfSegmentsInCache"));
        assertTrue(payload.supportedKeys()
                .contains("maxNumberOfKeysInActivePartition"));
        assertTrue(payload.supportedKeys()
                .contains("maxNumberOfKeysInPartitionBuffer"));
        assertTrue(payload.supportedKeys()
                .contains("maxNumberOfKeysInPartitionBeforeSplit"));
        assertFalse(payload.supportedKeys()
                .contains("backgroundMaintenanceAutoEnabled"));
        assertFalse(payload.original().containsKey("diskIoBufferSize"));
        assertFalse(
                payload.current().containsKey("numberOfIndexMaintenanceThreads"));
        assertFalse(
                payload.current().containsKey("backgroundMaintenanceAutoEnabled"));
    }

    @Test
    void configPatchRejectsOversizedBody() throws Exception {
        final String oversized = "x".repeat(1_100_000);
        final HttpResponse<String> response = send("PATCH",
                ManagementApiPaths.CONFIG, oversized);
        assertEquals(413, response.statusCode());
        final ErrorResponse payload = objectMapper.readValue(response.body(),
                ErrorResponse.class);
        assertEquals("REQUEST_TOO_LARGE", payload.code());
    }

    @Test
    void configPatchRejectsBackgroundMaintenanceFlag() throws Exception {
        final HttpResponse<String> response = send("PATCH",
                ManagementApiPaths.CONFIG + "?indexName=" + INDEX_1,
                "{\"values\":{\"backgroundMaintenanceAutoEnabled\":\"0\"},\"dryRun\":false}");
        assertEquals(400, response.statusCode());
        final ErrorResponse payload = objectMapper.readValue(response.body(),
                ErrorResponse.class);
        assertEquals("CONFIG_KEY_NOT_SUPPORTED", payload.code());
    }

    @Test
    void actionRejectsOversizedBody() throws Exception {
        final String oversized = "x".repeat(1_100_000);
        final HttpResponse<String> response = send("POST",
                ManagementApiPaths.ACTION_FLUSH, oversized);
        assertEquals(413, response.statusCode());
        final ErrorResponse payload = objectMapper.readValue(response.body(),
                ErrorResponse.class);
        assertEquals("REQUEST_TOO_LARGE", payload.code());
    }

    @Test
    void closedIndexIsAutomaticallyRemovedFromReport() throws Exception {
        indexes.get(1).close();

        final HttpResponse<String> reportResp = send("GET",
                ManagementApiPaths.REPORT, null);
        assertEquals(200, reportResp.statusCode());

        final NodeReportResponse report = objectMapper
                .readValue(reportResp.body(), NodeReportResponse.class);
        assertEquals(1, report.indexes().size());
        assertEquals(INDEX_1, report.indexes().get(0).indexName());
    }

    @Test
    void addAndRemoveIndexAffectsReport() throws Exception {
        assertTrue(server.removeIndex(INDEX_2));

        HttpResponse<String> reportResp = send("GET", ManagementApiPaths.REPORT,
                null);
        NodeReportResponse report = objectMapper.readValue(reportResp.body(),
                NodeReportResponse.class);
        assertEquals(1, report.indexes().size());

        final SegmentIndex<Integer, String> extra = createIndex("extra-index");
        indexes.add(extra);
        server.addIndex("extra-index", extra);

        reportResp = send("GET", ManagementApiPaths.REPORT, null);
        report = objectMapper.readValue(reportResp.body(),
                NodeReportResponse.class);
        assertEquals(2, report.indexes().size());
        assertTrue(report.indexes().stream()
                .anyMatch(idx -> idx.indexName().equals("extra-index")));
    }

    @Test
    void healthAndReadinessEndpointsReflectState() throws Exception {
        final HttpResponse<String> health = send("GET", "/health", null);
        assertEquals(200, health.statusCode());
        assertTrue(health.body().contains("\"UP\""));

        final HttpResponse<String> ready = send("GET", "/ready", null);
        assertEquals(200, ready.statusCode());
        assertTrue(ready.body().contains("\"READY\""));

        indexes.get(0).close();
        indexes.get(1).close();
        final HttpResponse<String> notReady = send("GET", "/ready", null);
        assertEquals(503, notReady.statusCode());
        assertTrue(notReady.body().contains("\"NOT_READY\""));
    }

    @Test
    void reportEndpointMapsBufferedOverlayMetricsFromSnapshot() throws Exception {
        final SegmentIndex<Integer, String> extra = createPartitionedIndex(
                "buffered-overlay-report-index", false);
        indexes.add(extra);
        server.addIndex(extra.getConfiguration().identity().name(), extra);

        for (int i = 0; i < 6; i++) {
            extra.put(i, "value-" + i);
        }
        final SegmentIndexMetricsSnapshot snapshot = extra.metricsSnapshot();

        final HttpResponse<String> reportResp = send("GET",
                ManagementApiPaths.REPORT, null);
        assertEquals(200, reportResp.statusCode());
        final JsonNode reportJson = objectMapper.readTree(reportResp.body());
        final JsonNode indexNode = findIndexNode(reportJson,
                extra.getConfiguration().identity().name());

        assertEquals(snapshot.getTotalBufferedWriteKeys(),
                indexNode.path("totalBufferedWriteKeys").asLong());
        assertEquals(snapshot.getLegacyPartitionCompatibilityMetrics().getPartitionCount(),
                indexNode.path("partitionCount").asInt());
        assertEquals(snapshot.getLegacyPartitionCompatibilityMetrics().getActivePartitionCount(),
                indexNode.path("activePartitionCount").asInt());
        assertEquals(snapshot.getLegacyPartitionCompatibilityMetrics().getDrainingPartitionCount(),
                indexNode.path("drainingPartitionCount").asInt());
        assertEquals(snapshot.getLegacyPartitionCompatibilityMetrics().getImmutableRunCount(),
                indexNode.path("immutableRunCount").asInt());
        assertEquals(snapshot.getLegacyPartitionCompatibilityMetrics().getPartitionBufferedKeyCount(),
                indexNode.path("partitionBufferedKeyCount").asInt());
        assertEquals(snapshot.getLegacyPartitionCompatibilityMetrics().getDrainScheduleCount(),
                indexNode.path("drainScheduleCount").asLong());
        assertEquals(snapshot.getLegacyPartitionCompatibilityMetrics().getDrainInFlightCount(),
                indexNode.path("drainInFlightCount").asInt());
    }

    @Test
    void reportEndpointMapsSplitMetricsFromSnapshot() throws Exception {
        final SegmentIndex<Integer, String> extra = createPartitionedIndex(
                "split-report-index", true);
        indexes.add(extra);
        server.addIndex(extra.getConfiguration().identity().name(), extra);

        for (int i = 0; i < 48; i++) {
            extra.put(i, "stable-" + i);
        }
        extra.flushAndWait();
        awaitCondition(() -> extra.metricsSnapshot().getSegmentCount() == 1
                && extra.metricsSnapshot().getSplitInFlightCount() == 0
                && extra.metricsSnapshot().getLegacyPartitionCompatibilityMetrics().getDrainInFlightCount() == 0,
                10_000L);

        final long revision = extra.controlPlane().configuration()
                .getConfigurationActual().getRevision();
        final RuntimePatchResult patchResult = extra.controlPlane()
                .configuration()
                .apply(new RuntimeConfigPatch(Map.of(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
                        Integer.valueOf(16)), false, Long.valueOf(revision)));
        assertTrue(patchResult.isApplied());

        awaitCondition(() -> {
            final SegmentIndexMetricsSnapshot snapshot = extra
                    .metricsSnapshot();
            return snapshot.getSegmentCount() > 1
                    && snapshot.getSplitInFlightCount() == 0
                    && snapshot.getLegacyPartitionCompatibilityMetrics().getDrainInFlightCount() == 0;
        }, 10_000L);

        final Map.Entry<SegmentIndexMetricsSnapshot, JsonNode> splitMetricsMatch =
                awaitIndexNodeWithMatchingSplitMetrics(extra,
                        extra.getConfiguration().identity().name(), 10_000L);
        final SegmentIndexMetricsSnapshot snapshot = splitMetricsMatch.getKey();
        final JsonNode indexNode = splitMetricsMatch.getValue();

        assertEquals(snapshot.getSegmentCount(),
                indexNode.path("segmentCount").asInt());
        assertEquals(snapshot.getSplitScheduleCount(),
                indexNode.path("splitScheduleCount").asLong());
        assertEquals(snapshot.getSplitInFlightCount(),
                indexNode.path("splitInFlightCount").asInt());
        assertEquals(snapshot.getLegacyPartitionCompatibilityMetrics().getDrainScheduleCount(),
                indexNode.path("drainScheduleCount").asLong());
        assertEquals(snapshot.getLegacyPartitionCompatibilityMetrics().getDrainInFlightCount(),
                indexNode.path("drainInFlightCount").asInt());
        assertEquals(snapshot.getLegacyPartitionCompatibilityMetrics().getDrainLatencyP95Micros(),
                indexNode.path("drainLatencyP95Micros").asLong());
    }

    private SegmentIndex<Integer, String> createIndex(final String name) {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class)
                        .valueClass(String.class)
                        .keyTypeDescriptor(new TypeDescriptorInteger())
                        .valueTypeDescriptor(new TypeDescriptorShortString())
                        .name(name)) //
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(0)) //
                .logging(logging -> logging.contextEnabled(false)) //
                .build();
        return SegmentIndex.create(directory, conf);
    }

    private SegmentIndex<Integer, String> createPartitionedIndex(
            final String name, final boolean backgroundMaintenanceAutoEnabled) {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class)
                        .valueClass(String.class)
                        .keyTypeDescriptor(new TypeDescriptorInteger())
                        .valueTypeDescriptor(new TypeDescriptorShortString())
                        .name(name)) //
                .segment(segment -> segment.cacheKeyLimit(8).maxKeys(128)
                        .chunkKeyLimit(4)) //
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(32)
                        .legacyImmutableRunLimit(2)
                        .maintenanceWriteCacheKeyLimit(96)
                        .indexBufferedWriteKeyLimit(192)
                        .segmentSplitKeyThreshold(512)) //
                .bloomFilter(bloomFilter -> bloomFilter
                        .indexSizeBytes(1024 * 128).hashFunctions(3)) //
                .logging(logging -> logging.contextEnabled(false)) //
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(
                        backgroundMaintenanceAutoEnabled)) //
                .build();
        return SegmentIndex.create(directory, conf);
    }

    private JsonNode findIndexNode(final JsonNode reportJson,
            final String indexName) {
        for (final JsonNode indexNode : reportJson.path("indexes")) {
            if (indexName.equals(indexNode.path("indexName").asText())) {
                return indexNode;
            }
        }
        return fail("Missing index report for " + indexName);
    }

    private static void awaitCondition(final Supplier<Boolean> condition,
            final long timeoutMillis) {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.get()) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20L));
            if (Thread.currentThread().isInterrupted()) {
                throw new AssertionError("Interrupted while waiting");
            }
        }
        assertTrue(condition.get(),
                "Condition not reached within " + timeoutMillis + " ms.");
    }

    private static boolean sameSplitMetrics(
            final SegmentIndexMetricsSnapshot left,
            final SegmentIndexMetricsSnapshot right) {
        return left != null && right != null
                && left.getSegmentCount() == right.getSegmentCount()
                && left.getSplitScheduleCount() == right.getSplitScheduleCount()
                && left.getSplitInFlightCount() == right.getSplitInFlightCount()
                && left.getLegacyPartitionCompatibilityMetrics().getDrainScheduleCount() == right.getLegacyPartitionCompatibilityMetrics().getDrainScheduleCount()
                && left.getLegacyPartitionCompatibilityMetrics().getDrainInFlightCount() == right.getLegacyPartitionCompatibilityMetrics().getDrainInFlightCount()
                && left.getLegacyPartitionCompatibilityMetrics().getDrainLatencyP95Micros() == right.getLegacyPartitionCompatibilityMetrics()
                        .getDrainLatencyP95Micros();
    }

    private Map.Entry<SegmentIndexMetricsSnapshot, JsonNode> awaitIndexNodeWithMatchingSplitMetrics(
            final SegmentIndex<Integer, String> index,
            final String indexName, final long timeoutMillis)
            throws Exception {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        SegmentIndexMetricsSnapshot lastSnapshot = null;
        JsonNode lastIndexNode = null;
        while (System.nanoTime() < deadline) {
            final SegmentIndexMetricsSnapshot before = index.metricsSnapshot();
            final HttpResponse<String> reportResp = send("GET",
                    ManagementApiPaths.REPORT, null);
            assertEquals(200, reportResp.statusCode());
            final JsonNode reportJson = objectMapper.readTree(reportResp.body());
            final JsonNode indexNode = findIndexNode(reportJson, indexName);
            final SegmentIndexMetricsSnapshot after = index.metricsSnapshot();
            if (sameSplitMetrics(before, after)
                    && splitMetricsMatch(indexNode, after)) {
                return Map.entry(after, indexNode);
            }
            lastSnapshot = after;
            lastIndexNode = indexNode;
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20L));
            if (Thread.currentThread().isInterrupted()) {
                throw new AssertionError("Interrupted while waiting");
            }
        }
        assertNotNull(lastSnapshot,
                "Split metrics snapshot was never observed while waiting.");
        assertNotNull(lastIndexNode,
                "Index report was never observed while waiting.");
        assertTrue(false,
                "Index report split metrics did not match a stable snapshot within "
                        + timeoutMillis + " ms.");
        return Map.entry(lastSnapshot, lastIndexNode);
    }

    private static boolean splitMetricsMatch(final JsonNode indexNode,
            final SegmentIndexMetricsSnapshot snapshot) {
        return snapshot.getSegmentCount() == indexNode.path("segmentCount")
                .asInt()
                && snapshot.getSplitScheduleCount() == indexNode
                        .path("splitScheduleCount").asLong()
                && snapshot.getSplitInFlightCount() == indexNode
                        .path("splitInFlightCount").asInt()
                && snapshot.getLegacyPartitionCompatibilityMetrics().getDrainScheduleCount() == indexNode
                        .path("drainScheduleCount").asLong()
                && snapshot.getLegacyPartitionCompatibilityMetrics().getDrainInFlightCount() == indexNode
                        .path("drainInFlightCount").asInt()
                && snapshot.getLegacyPartitionCompatibilityMetrics().getDrainLatencyP95Micros() == indexNode
                        .path("drainLatencyP95Micros").asLong();
    }

    @SuppressWarnings("unchecked")
    private SegmentIndex<Integer, String> instrumentAction(
            final SegmentIndex<Integer, String> delegate,
            final String actionMethodName,
            final AtomicInteger invocationCount,
            final RuntimeException failure) {
        final InvocationHandler handler = (proxy, method, args) -> {
            if (actionMethodName.equals(method.getName())) {
                invocationCount.incrementAndGet();
                if (failure != null) {
                    throw failure;
                }
            }
            try {
                return method.invoke(delegate, args);
            } catch (final InvocationTargetException e) {
                throw e.getCause();
            }
        };
        return (SegmentIndex<Integer, String>) Proxy.newProxyInstance(
                SegmentIndex.class.getClassLoader(),
                new Class<?>[] { SegmentIndex.class }, handler);
    }

    private HttpResponse<String> send(final String method, final String path,
            final String body) throws Exception {
        HttpRequest.BodyPublisher publisher = HttpRequest.BodyPublishers
                .noBody();
        if (body != null) {
            publisher = HttpRequest.BodyPublishers.ofString(body);
        }
        final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("Content-Type", "application/json")
                .method(method, publisher).build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static String[][] invalidConfigPatchRequests() {
        return new String[][] {
                { ManagementApiPaths.CONFIG + "?indexName=" + INDEX_1,
                        "{\"values\":{\"forbidden.key\":\"1\"},\"dryRun\":false}",
                        "CONFIG_KEY_NOT_SUPPORTED" },
                { ManagementApiPaths.CONFIG + "?indexName=" + INDEX_1,
                        "{\"values\":{\"maxNumberOfKeysInSegmentWriteCache\":\"16\"},\"dryRun\":false}",
                        "CONFIG_KEY_NOT_SUPPORTED" },
                { ManagementApiPaths.CONFIG,
                        "{\"values\":{\"maxNumberOfSegmentsInCache\":\"16\"},\"dryRun\":true}",
                        "INVALID_REQUEST" } };
    }
}
