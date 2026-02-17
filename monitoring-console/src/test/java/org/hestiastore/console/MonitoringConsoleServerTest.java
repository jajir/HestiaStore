package org.hestiastore.console;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.management.agent.ManagementAgentServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpServer;

class MonitoringConsoleServerTest {

    private static final String TOKEN = "console-secret";

    private final List<SegmentIndex<Integer, String>> indexes = new ArrayList<>();
    private final List<ManagementAgentServer> agents = new ArrayList<>();
    private final List<HttpServer> stubs = new ArrayList<>();

    private MonitoringConsoleServer console;
    private String baseUrl;
    private HttpClient client;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() throws Exception {
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2))
                .build();
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        console = new MonitoringConsoleServer("127.0.0.1", 0, TOKEN);
        console.start();
        baseUrl = "http://127.0.0.1:" + console.getPort();
    }

    @AfterEach
    void tearDown() {
        if (console != null) {
            console.close();
        }
        for (final ManagementAgentServer agent : agents) {
            agent.close();
        }
        for (final SegmentIndex<Integer, String> index : indexes) {
            if (!index.wasClosed()) {
                index.close();
            }
        }
        for (final HttpServer stub : stubs) {
            stub.stop(0);
        }
    }

    @Test
    void dashboardSupportsThreeNodes() throws Exception {
        final ManagedNode n1 = startNode("n1", "index-1");
        final ManagedNode n2 = startNode("n2", "index-2");
        final ManagedNode n3 = startNode("n3", "index-3");
        n1.index().put(1, "A");
        n2.index().put(2, "B");
        n3.index().put(3, "C");

        registerNode("n1", "index-1", n1.baseUrl());
        registerNode("n2", "index-2", n2.baseUrl());
        registerNode("n3", "index-3", n3.baseUrl());

        final HttpResponse<String> response = send("GET", "/console/v1/dashboard",
                null, null);
        assertEquals(200, response.statusCode());
        final JsonNode array = objectMapper.readTree(response.body());
        assertEquals(3, array.size());
        for (final JsonNode node : array) {
            assertTrue(node.get("reachable").asBoolean());
            assertTrue(node.get("pollLatencyMillis").asLong() >= 0L);
        }
    }

    @Test
    void actionLifecycleShowsSuccessAndFailure() throws Exception {
        final ManagedNode live = startNode("live", "index-live");
        live.index().put(1, "A");
        registerNode("live", "index-live", live.baseUrl());
        registerNode("dead", "index-dead", "http://127.0.0.1:1");

        final String successActionId = submitAction("/console/v1/actions/flush",
                "live", "req-success");
        final JsonNode success = waitForTerminal(successActionId);
        assertEquals("SUCCESS", success.get("status").asText());

        final String failedActionId = submitAction("/console/v1/actions/compact",
                "dead", "req-failed");
        final JsonNode failed = waitForTerminal(failedActionId);
        assertEquals("FAILED", failed.get("status").asText());
    }

    @Test
    void writeEndpointsRequireToken() throws Exception {
        final HttpResponse<String> forbidden = send("POST", "/console/v1/nodes",
                """
                        {"nodeId":"a","nodeName":"n","baseUrl":"http://127.0.0.1:7"}
                        """,
                null);
        assertEquals(403, forbidden.statusCode());
    }

    @Test
    void dashboardMarksNodeUnavailableWhenNodeIsDown() throws Exception {
        registerNode("dead", "index-dead", "http://127.0.0.1:1");

        final HttpResponse<String> response = send("GET", "/console/v1/dashboard",
                null, null);
        assertEquals(200, response.statusCode());
        final JsonNode payload = objectMapper.readTree(response.body());
        assertEquals(1, payload.size());
        assertEquals("dead", payload.get(0).get("nodeId").asText());
        assertEquals(false, payload.get(0).get("reachable").asBoolean());
        assertEquals("UNAVAILABLE", payload.get(0).get("state").asText());
    }

    @Test
    void dashboardMarksNodeUnavailableOnPartialOrTimeoutResponses()
            throws Exception {
        final HttpServer partial = HttpServer.create(new InetSocketAddress(0), 0);
        partial.createContext("/api/v1/state", exchange -> {
            final byte[] ok = """
                    {"indexName":"partial","state":"READY","ready":true,"capturedAt":"2026-01-01T00:00:00Z"}
                    """.getBytes();
            exchange.sendResponseHeaders(200, ok.length);
            exchange.getResponseBody().write(ok);
            exchange.close();
        });
        partial.createContext("/api/v1/metrics", exchange -> {
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        partial.start();
        stubs.add(partial);

        final HttpServer timeout = HttpServer.create(new InetSocketAddress(0), 0);
        timeout.createContext("/api/v1/state", exchange -> {
            try {
                Thread.sleep(3500L);
                final byte[] ok = """
                        {"indexName":"timeout","state":"READY","ready":true,"capturedAt":"2026-01-01T00:00:00Z"}
                        """.getBytes();
                exchange.sendResponseHeaders(200, ok.length);
                exchange.getResponseBody().write(ok);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                exchange.close();
            }
        });
        timeout.createContext("/api/v1/metrics", exchange -> {
            final byte[] ok = """
                    {"indexName":"timeout","state":"READY","getOperationCount":1,"putOperationCount":1,"deleteOperationCount":0,"capturedAt":"2026-01-01T00:00:00Z"}
                    """.getBytes();
            exchange.sendResponseHeaders(200, ok.length);
            exchange.getResponseBody().write(ok);
            exchange.close();
        });
        timeout.start();
        stubs.add(timeout);

        registerNode("partial", "partial", "http://127.0.0.1:" + partial.getAddress().getPort());
        registerNode("timeout", "timeout", "http://127.0.0.1:" + timeout.getAddress().getPort());

        final HttpResponse<String> response = send("GET", "/console/v1/dashboard",
                null, null);
        assertEquals(200, response.statusCode());
        final JsonNode payload = objectMapper.readTree(response.body());
        assertEquals(2, payload.size());
        for (final JsonNode node : payload) {
            assertEquals(false, node.get("reachable").asBoolean());
            assertEquals("UNAVAILABLE", node.get("state").asText());
        }
    }

    private ManagedNode startNode(final String nodeId, final String name)
            throws IOException {
        final SegmentIndex<Integer, String> index = createIndex(name);
        indexes.add(index);
        final ManagementAgentServer agent = new ManagementAgentServer("127.0.0.1",
                0, index, name, Set.of("indexBusyTimeoutMillis"));
        agents.add(agent);
        agent.start();
        return new ManagedNode(nodeId, index, "http://127.0.0.1:" + agent.getPort());
    }

    private SegmentIndex<Integer, String> createIndex(final String name)
            throws IOException {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withBloomFilterIndexSizeInBytes(0)//
                .withContextLoggingEnabled(false)//
                .withName(name)//
                .build();
        return SegmentIndex.create(directory, conf);
    }

    private void registerNode(final String nodeId, final String nodeName,
            final String nodeBaseUrl) throws Exception {
        final HttpResponse<String> response = send("POST", "/console/v1/nodes",
                """
                        {"nodeId":"%s","nodeName":"%s","baseUrl":"%s"}
                        """.formatted(nodeId, nodeName, nodeBaseUrl),
                TOKEN);
        assertEquals(201, response.statusCode());
    }

    private String submitAction(final String path, final String nodeId,
            final String requestId) throws Exception {
        final HttpResponse<String> response = send("POST", path,
                """
                        {"nodeId":"%s","requestId":"%s","confirmed":true}
                        """.formatted(nodeId, requestId),
                TOKEN);
        assertEquals(202, response.statusCode());
        return objectMapper.readTree(response.body()).get("actionId").asText();
    }

    private JsonNode waitForTerminal(final String actionId) throws Exception {
        final long deadline = System.currentTimeMillis() + 5000L;
        while (System.currentTimeMillis() < deadline) {
            final HttpResponse<String> response = send("GET",
                    "/console/v1/actions/" + actionId, null, null);
            assertEquals(200, response.statusCode());
            final JsonNode payload = objectMapper.readTree(response.body());
            final String status = payload.get("status").asText();
            if (!"PENDING".equals(status)) {
                return payload;
            }
            Thread.sleep(50L);
        }
        throw new IllegalStateException("Action did not reach terminal state");
    }

    private HttpResponse<String> send(final String method, final String path,
            final String body, final String token) throws Exception {
        HttpRequest.BodyPublisher publisher = HttpRequest.BodyPublishers.noBody();
        if (body != null) {
            publisher = HttpRequest.BodyPublishers.ofString(body);
        }
        final HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("Content-Type", "application/json");
        if (token != null) {
            builder.header("X-Hestia-Console-Token", token);
        }
        final HttpRequest request = builder.method(method, publisher).build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private record ManagedNode(String nodeId, SegmentIndex<Integer, String> index,
            String baseUrl) {
    }
}
