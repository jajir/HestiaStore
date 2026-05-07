package org.hestiastore.console.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

class ConsoleBackendClientTest {

    private HttpServer server;
    private AtomicReference<String> reportPayload;
    private ConsoleBackendClient client;

    @BeforeEach
    void setUp() throws IOException {
        reportPayload = new AtomicReference<>(singleIndexReportJson());
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/report", this::handleReportRequest);
        server.start();
        client = new ConsoleBackendClient(new MonitoringConsoleWebProperties(
                "", List.of(new MonitoringConsoleWebProperties.NodeEndpoint(
                        "node-1", "Node 1",
                        "http://127.0.0.1:" + server.getAddress().getPort(),
                        "")),
                1_000L));
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void fetchNodeDetailsParsesPartitionAndSplitMetricsFromReport() {
        final Optional<ConsoleBackendClient.NodeDetails> detailsOptional = client
                .fetchNodeDetails("node-1");

        assertTrue(detailsOptional.isPresent());
        final ConsoleBackendClient.NodeDetails details = detailsOptional
                .orElseThrow();
        assertNode(details.node());
        assertEquals(1, details.indexes().size());
        assertIndexRow(details.indexes().get(0));
    }

    private void handleReportRequest(final HttpExchange exchange)
            throws IOException {
        final byte[] body = reportPayload.get().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
        } finally {
            exchange.close();
        }
    }

    private String singleIndexReportJson() {
        return """
                {
                  "jvm": {
                    "heapUsedBytes": 1,
                    "heapCommittedBytes": 2,
                    "heapMaxBytes": 3,
                    "nonHeapUsedBytes": 4,
                    "gcCount": 5,
                    "gcTimeMillis": 6
                  },
                  "indexes": [
                    {
                      "indexName": "orders",
                      "state": "READY",
                      "ready": true,
                      "getOperationCount": 11,
                      "putOperationCount": 12,
                      "deleteOperationCount": 13,
                      "registryCacheHitCount": 14,
                      "registryCacheMissCount": 15,
                      "registryCacheLoadCount": 16,
                      "registryCacheEvictionCount": 17,
                      "registryCacheSize": 18,
                      "registryCacheLimit": 19,
                      "segmentCacheKeyLimitPerSegment": 20,
                      "segmentWriteCacheKeyLimit": 7,
                      "segmentWriteCacheKeyLimitDuringMaintenance": 11,
                      "indexBufferedWriteKeyLimit": 29,
                      "segmentCount": 3,
                      "segmentReadyCount": 2,
                      "segmentMaintenanceCount": 1,
                      "segmentErrorCount": 0,
                      "segmentClosedCount": 0,
                      "segmentBusyCount": 0,
                      "totalSegmentKeys": 21,
                      "totalSegmentCacheKeys": 22,
                      "totalBufferedWriteKeys": 23,
                      "totalDeltaCacheFiles": 24,
                      "compactRequestCount": 25,
                      "flushRequestCount": 26,
                      "splitScheduleCount": 37,
                      "splitInFlightCount": 2,
                      "maintenanceQueueSize": 27,
                      "maintenanceQueueCapacity": 28,
                      "splitQueueSize": 29,
                      "splitQueueCapacity": 30,
                      "readLatencyP50Micros": 31,
                      "readLatencyP95Micros": 32,
                      "readLatencyP99Micros": 33,
                      "writeLatencyP50Micros": 34,
                      "writeLatencyP95Micros": 35,
                      "writeLatencyP99Micros": 36,
                      "bloomFilterHashFunctions": 3,
                      "bloomFilterIndexSizeInBytes": 1024,
                      "bloomFilterProbabilityOfFalsePositive": 0.01,
                      "bloomFilterRequestCount": 38,
                      "bloomFilterRefusedCount": 39,
                      "bloomFilterPositiveCount": 40,
                      "bloomFilterFalsePositiveCount": 41,
                      "segmentRuntimeSnapshots": [
                        {
                          "segmentId": "seg-1",
                          "state": "READY",
                          "numberOfKeysInDeltaCache": 1,
                          "numberOfKeysInSegment": 2,
                          "numberOfKeysInScarceIndex": 3,
                          "numberOfKeysInSegmentCache": 4,
                          "numberOfKeysInWriteCache": 5,
                          "numberOfDeltaCacheFiles": 6,
                          "compactRequestCount": 7,
                          "flushRequestCount": 8,
                          "bloomFilterRequestCount": 9,
                          "bloomFilterRefusedCount": 10,
                          "bloomFilterPositiveCount": 11,
                          "bloomFilterFalsePositiveCount": 12
                        }
                      ]
                    }
                  ],
                  "capturedAt": "2026-03-12T12:00:00Z"
                }
                """;
    }

    private static void assertNode(final ConsoleBackendClient.NodeRow node) {
        assertEquals("READY", node.state());
        assertTrue(node.reachable());
        assertTrue(node.ready());
        assertEquals("orders", node.indexName());
        assertEquals(37L, node.splitScheduleCount());
        assertEquals(2, node.splitInFlightCount());
    }

    private static void assertIndexRow(final ConsoleBackendClient.IndexRow row) {
        assertEquals("orders", row.indexName());
        assertEquals("READY", row.state());
        assertTrue(row.ready());
        assertEquals(7, row.segmentWriteCacheKeyLimit());
        assertEquals(11, row.segmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(29, row.indexBufferedWriteKeyLimit());
        assertEquals(3, row.segmentCount());
        assertEquals(2, row.segmentReadyCount());
        assertEquals(1, row.segmentMaintenanceCount());
        assertEquals(37L, row.splitScheduleCount());
        assertEquals(2, row.splitInFlightCount());
        assertEquals(1, row.segmentRuntimeSnapshots().size());
        assertEquals("seg-1", row.segmentRuntimeSnapshots().get(0).segmentId());
    }
}
