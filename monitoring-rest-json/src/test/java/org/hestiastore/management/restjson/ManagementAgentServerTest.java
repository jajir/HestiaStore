package org.hestiastore.management.restjson;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
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
                .allMatch(idx -> idx.partitionCount() >= 1));
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
    void configPatchRejectsForbiddenKey() throws Exception {
        final HttpResponse<String> response = send("PATCH",
                ManagementApiPaths.CONFIG + "?indexName=" + INDEX_1,
                "{\"values\":{\"forbidden.key\":\"1\"},\"dryRun\":false}");
        assertEquals(400, response.statusCode());
        final ErrorResponse payload = objectMapper.readValue(response.body(),
                ErrorResponse.class);
        assertEquals("CONFIG_KEY_NOT_SUPPORTED", payload.code());
    }

    @Test
    void configPatchRejectsLegacyPartitionAliasKey() throws Exception {
        final HttpResponse<String> response = send("PATCH",
                ManagementApiPaths.CONFIG + "?indexName=" + INDEX_1,
                "{\"values\":{\"maxNumberOfKeysInSegmentWriteCache\":\"16\"},\"dryRun\":false}");
        assertEquals(400, response.statusCode());
        final ErrorResponse payload = objectMapper.readValue(response.body(),
                ErrorResponse.class);
        assertEquals("CONFIG_KEY_NOT_SUPPORTED", payload.code());
    }

    @Test
    void configPatchRequiresIndexName() throws Exception {
        final HttpResponse<String> response = send("PATCH",
                ManagementApiPaths.CONFIG,
                "{\"values\":{\"maxNumberOfSegmentsInCache\":\"16\"},\"dryRun\":true}");
        assertEquals(400, response.statusCode());
        final ErrorResponse payload = objectMapper.readValue(response.body(),
                ErrorResponse.class);
        assertEquals("INVALID_REQUEST", payload.code());
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
        server.addIndex(extra.getConfiguration().getIndexName(), extra);

        for (int i = 0; i < 6; i++) {
            extra.put(i, "value-" + i);
        }
        final SegmentIndexMetricsSnapshot snapshot = extra.metricsSnapshot();

        final HttpResponse<String> reportResp = send("GET",
                ManagementApiPaths.REPORT, null);
        assertEquals(200, reportResp.statusCode());
        final JsonNode reportJson = objectMapper.readTree(reportResp.body());
        final JsonNode indexNode = findIndexNode(reportJson,
                extra.getConfiguration().getIndexName());

        assertEquals(snapshot.getTotalBufferedWriteKeys(),
                indexNode.path("totalBufferedWriteKeys").asLong());
        assertEquals(snapshot.getPartitionCount(),
                indexNode.path("partitionCount").asInt());
        assertEquals(snapshot.getActivePartitionCount(),
                indexNode.path("activePartitionCount").asInt());
        assertEquals(snapshot.getDrainingPartitionCount(),
                indexNode.path("drainingPartitionCount").asInt());
        assertEquals(snapshot.getImmutableRunCount(),
                indexNode.path("immutableRunCount").asInt());
        assertEquals(snapshot.getPartitionBufferedKeyCount(),
                indexNode.path("partitionBufferedKeyCount").asInt());
        assertEquals(snapshot.getDrainScheduleCount(),
                indexNode.path("drainScheduleCount").asLong());
        assertEquals(snapshot.getDrainInFlightCount(),
                indexNode.path("drainInFlightCount").asInt());
    }

    @Test
    void reportEndpointMapsSplitMetricsFromSnapshot() throws Exception {
        final SegmentIndex<Integer, String> extra = createPartitionedIndex(
                "split-report-index", true);
        indexes.add(extra);
        server.addIndex(extra.getConfiguration().getIndexName(), extra);

        for (int i = 0; i < 48; i++) {
            extra.put(i, "stable-" + i);
        }
        extra.flushAndWait();
        awaitCondition(() -> extra.metricsSnapshot().getSegmentCount() == 1
                && extra.metricsSnapshot().getSplitInFlightCount() == 0
                && extra.metricsSnapshot().getDrainInFlightCount() == 0,
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
                    && snapshot.getDrainInFlightCount() == 0;
        }, 10_000L);

        final SegmentIndexMetricsSnapshot snapshot = extra.metricsSnapshot();
        final HttpResponse<String> reportResp = send("GET",
                ManagementApiPaths.REPORT, null);
        assertEquals(200, reportResp.statusCode());
        final JsonNode reportJson = objectMapper.readTree(reportResp.body());
        final JsonNode indexNode = findIndexNode(reportJson,
                extra.getConfiguration().getIndexName());

        assertEquals(snapshot.getSegmentCount(),
                indexNode.path("segmentCount").asInt());
        assertEquals(snapshot.getSplitScheduleCount(),
                indexNode.path("splitScheduleCount").asLong());
        assertEquals(snapshot.getSplitInFlightCount(),
                indexNode.path("splitInFlightCount").asInt());
        assertEquals(snapshot.getDrainScheduleCount(),
                indexNode.path("drainScheduleCount").asLong());
        assertEquals(snapshot.getDrainInFlightCount(),
                indexNode.path("drainInFlightCount").asInt());
        assertEquals(snapshot.getDrainLatencyP95Micros(),
                indexNode.path("drainLatencyP95Micros").asLong());
    }

    private SegmentIndex<Integer, String> createIndex(final String name) {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger()) //
                .withValueTypeDescriptor(new TypeDescriptorShortString()) //
                .withBloomFilterIndexSizeInBytes(0) //
                .withContextLoggingEnabled(false) //
                .withName(name) //
                .build();
        return SegmentIndex.create(directory, conf);
    }

    private SegmentIndex<Integer, String> createPartitionedIndex(
            final String name, final boolean backgroundMaintenanceAutoEnabled) {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger()) //
                .withValueTypeDescriptor(new TypeDescriptorShortString()) //
                .withMaxNumberOfKeysInSegmentCache(8) //
                .withMaxNumberOfKeysInActivePartition(32) //
                .withMaxNumberOfImmutableRunsPerPartition(2) //
                .withMaxNumberOfKeysInPartitionBuffer(96) //
                .withMaxNumberOfKeysInIndexBuffer(192) //
                .withMaxNumberOfKeysInPartitionBeforeSplit(512) //
                .withMaxNumberOfKeysInSegment(128) //
                .withMaxNumberOfKeysInSegmentChunk(4) //
                .withBloomFilterIndexSizeInBytes(1024 * 128) //
                .withBloomFilterNumberOfHashFunctions(3) //
                .withContextLoggingEnabled(false) //
                .withBackgroundMaintenanceAutoEnabled(
                        backgroundMaintenanceAutoEnabled) //
                .withName(name) //
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
        assertNotNull(null, "Missing index report for " + indexName);
        return null;
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
}
