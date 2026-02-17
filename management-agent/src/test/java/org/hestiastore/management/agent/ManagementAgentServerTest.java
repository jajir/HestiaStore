package org.hestiastore.management.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Set;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.management.api.ActionResponse;
import org.hestiastore.management.api.ActionStatus;
import org.hestiastore.management.api.ActionType;
import org.hestiastore.management.api.ErrorResponse;
import org.hestiastore.management.api.ManagementApiPaths;
import org.hestiastore.management.api.MetricsResponse;
import org.hestiastore.management.api.NodeStateResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

class ManagementAgentServerTest {

    private static final String INDEX_NAME = "agent-test-index";

    private SegmentIndex<Integer, String> index;
    private ManagementAgentServer server;
    private HttpClient client;
    private ObjectMapper objectMapper;
    private String baseUrl;

    @BeforeEach
    void setUp() throws IOException {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger()) //
                .withValueTypeDescriptor(new TypeDescriptorShortString()) //
                .withBloomFilterIndexSizeInBytes(0) //
                .withContextLoggingEnabled(false) //
                .withName(INDEX_NAME) //
                .build();
        index = SegmentIndex.create(directory, conf);
        server = new ManagementAgentServer("127.0.0.1", 0, index, INDEX_NAME,
                Set.of("indexBusyTimeoutMillis"));
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
        if (index != null && !index.wasClosed()) {
            index.close();
        }
    }

    @Test
    void stateAndMetricsEndpointsReturnPayloads() throws Exception {
        index.put(1, "A");
        assertEquals("A", index.get(1));

        final HttpResponse<String> stateResp = send("GET",
                ManagementApiPaths.STATE, null);
        assertEquals(200, stateResp.statusCode());
        final NodeStateResponse state = objectMapper.readValue(stateResp.body(),
                NodeStateResponse.class);
        assertEquals(INDEX_NAME, state.indexName());
        assertEquals("READY", state.state());
        assertTrue(state.ready());

        final HttpResponse<String> metricsResp = send("GET",
                ManagementApiPaths.METRICS, null);
        assertEquals(200, metricsResp.statusCode());
        final MetricsResponse metrics = objectMapper
                .readValue(metricsResp.body(), MetricsResponse.class);
        assertEquals(INDEX_NAME, metrics.indexName());
        assertEquals("READY", metrics.state());
        assertTrue(metrics.putOperationCount() >= 1L);
        assertTrue(metrics.getOperationCount() >= 1L);
    }

    @Test
    void flushActionReturnsCompleted() throws Exception {
        index.put(1, "A");
        final HttpResponse<String> response = send("POST",
                ManagementApiPaths.ACTION_FLUSH, "{\"requestId\":\"req-1\"}");
        assertEquals(200, response.statusCode());
        final ActionResponse payload = objectMapper.readValue(response.body(),
                ActionResponse.class);
        assertEquals("req-1", payload.requestId());
        assertEquals(ActionType.FLUSH, payload.action());
        assertEquals(ActionStatus.COMPLETED, payload.status());
    }

    @Test
    void compactActionReturnsCompleted() throws Exception {
        index.put(1, "A");
        final HttpResponse<String> response = send("POST",
                ManagementApiPaths.ACTION_COMPACT,
                "{\"requestId\":\"req-compact\"}");
        assertEquals(200, response.statusCode());
        final ActionResponse payload = objectMapper.readValue(response.body(),
                ActionResponse.class);
        assertEquals("req-compact", payload.requestId());
        assertEquals(ActionType.COMPACT, payload.action());
        assertEquals(ActionStatus.COMPLETED, payload.status());
    }

    @Test
    void configPatchRejectsForbiddenKey() throws Exception {
        final HttpResponse<String> response = send("PATCH",
                ManagementApiPaths.CONFIG,
                "{\"values\":{\"forbidden.key\":\"1\"},\"dryRun\":false}");
        assertEquals(400, response.statusCode());
        final ErrorResponse payload = objectMapper.readValue(response.body(),
                ErrorResponse.class);
        assertEquals("CONFIG_KEY_NOT_ALLOWED", payload.code());
    }

    @Test
    void mutatingActionRejectsWhenIndexIsClosed() throws Exception {
        index.close();
        final HttpResponse<String> response = send("POST",
                ManagementApiPaths.ACTION_COMPACT,
                "{\"requestId\":\"req-close\"}");
        assertEquals(409, response.statusCode());
        final ErrorResponse payload = objectMapper.readValue(response.body(),
                ErrorResponse.class);
        assertEquals("INVALID_STATE", payload.code());
        assertEquals("req-close", payload.requestId());
    }

    @Test
    void healthAndReadinessEndpointsReflectState() throws Exception {
        final HttpResponse<String> health = send("GET", "/health", null);
        assertEquals(200, health.statusCode());
        assertTrue(health.body().contains("\"UP\""));

        final HttpResponse<String> ready = send("GET", "/ready", null);
        assertEquals(200, ready.statusCode());
        assertTrue(ready.body().contains("\"READY\""));

        index.close();
        final HttpResponse<String> notReady = send("GET", "/ready", null);
        assertEquals(503, notReady.statusCode());
        assertTrue(notReady.body().contains("\"NOT_READY\""));
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
