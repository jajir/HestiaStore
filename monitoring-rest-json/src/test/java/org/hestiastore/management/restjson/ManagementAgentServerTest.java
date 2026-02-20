package org.hestiastore.management.restjson;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

class ManagementAgentServerTest {

    private static final String INDEX_1 = "agent-test-index-1";
    private static final String INDEX_2 = "agent-test-index-2";
    private static final Set<String> RUNTIME_ALLOWLIST = Set.of(
            "maxNumberOfSegmentsInCache",
            "maxNumberOfKeysInSegmentCache",
            "maxNumberOfKeysInSegmentWriteCache",
            "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance");

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

        server = new ManagementAgentServer("127.0.0.1", 0, index1, INDEX_1,
                RUNTIME_ALLOWLIST);
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

        final NodeReportResponse report = objectMapper.readValue(
                reportResp.body(), NodeReportResponse.class);
        assertEquals(2, report.indexes().size());
        assertTrue(report.indexes().stream()
                .anyMatch(idx -> idx.indexName().equals(INDEX_1)));
        assertTrue(report.indexes().stream()
                .anyMatch(idx -> idx.indexName().equals(INDEX_2)));
        assertTrue(report.jvm().heapUsedBytes() >= 0L);
        assertTrue(report.jvm().heapMaxBytes() >= 0L);
        assertTrue(report.jvm().gcCount() >= 0L);
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
                "{\"requestId\":\"req-compact\",\"indexName\":\""
                        + INDEX_2 + "\"}");
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
    void configGetReturnsOriginalAndCurrentValues() throws Exception {
        final HttpResponse<String> response = send("GET",
                ManagementApiPaths.CONFIG + "?indexName=" + INDEX_1, null);
        assertEquals(200, response.statusCode());
        final ConfigViewResponse payload = objectMapper.readValue(
                response.body(), ConfigViewResponse.class);
        assertEquals(INDEX_1, payload.indexName());
        assertTrue(payload.original().containsKey("maxNumberOfSegmentsInCache"));
        assertTrue(payload.current()
                .containsKey("maxNumberOfKeysInSegmentCache"));
        assertTrue(payload.supportedKeys()
                .contains("maxNumberOfSegmentsInCache"));
        assertTrue(payload.supportedKeys().contains(
                "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance"));
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

        final NodeReportResponse report = objectMapper.readValue(
                reportResp.body(), NodeReportResponse.class);
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

    private SegmentIndex<Integer, String> createIndex(final String name)
            throws IOException {
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
