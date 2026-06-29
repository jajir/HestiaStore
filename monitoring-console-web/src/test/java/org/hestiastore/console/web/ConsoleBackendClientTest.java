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
                      "operations": {
                        "readOperationCount": 11,
                        "putOperationCount": 12,
                        "deleteOperationCount": 13
                      },
                      "registryCache": {
                        "hitCount": 14,
                        "missCount": 15,
                        "loadCount": 16,
                        "evictionCount": 17,
                        "size": 18,
                        "limit": 19
                      },
                      "chunkStoreCache": {
                        "pageLimit": 42,
                        "pageCount": 43,
                        "entryCount": 44,
                        "hitCount": 45,
                        "missCount": 46,
                        "loadCount": 47,
                        "evictionCount": 48,
                        "invalidationCount": 49
                      },
                      "segments": {
                        "cacheKeyLimitPerSegment": 20,
                        "count": 3,
                        "readyCount": 2,
                        "maintenanceCount": 1,
                        "errorCount": 0,
                        "closedCount": 0,
                        "unloadedMappedSegmentCount": 0,
                        "totalKeys": 21,
                        "totalCacheKeys": 22,
                        "totalDeltaCacheFiles": 24,
                        "runtimeMetrics": [
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
                      },
                      "writePath": {
                        "segmentWriteCacheKeyLimit": 7,
                        "segmentWriteCacheKeyLimitDuringMaintenance": 11,
                        "indexBufferedWriteKeyLimit": 29,
                        "totalBufferedWriteKeys": 23
                      },
                      "maintenance": {
                        "compactRequestCount": 25,
                        "flushRequestCount": 26,
                        "flushAcceptedToReadyP95Micros": 53,
                        "compactAcceptedToReadyP95Micros": 54,
                        "flushBusyRetryCount": 55,
                        "compactBusyRetryCount": 56,
                        "indexExecutor": {
                          "activeThreadCount": 0,
                          "queueSize": 27,
                          "queueCapacity": 28,
                          "completedTaskCount": 57,
                          "rejectedTaskCount": 58,
                          "callerRunsCount": 59
                        },
                        "stableSegmentExecutor": {
                          "activeThreadCount": 0,
                          "queueSize": 0,
                          "queueCapacity": 0,
                          "completedTaskCount": 0,
                          "rejectedTaskCount": 0,
                          "callerRunsCount": 0
                        }
                      },
                      "split": {
                        "scheduleCount": 37,
                        "inFlightCount": 2,
                        "blockedCount": 0,
                        "taskStartDelayP95Micros": 60,
                        "taskRunLatencyP95Micros": 61,
                        "executor": {
                          "activeThreadCount": 0,
                          "queueSize": 29,
                          "queueCapacity": 30,
                          "completedTaskCount": 62,
                          "rejectedTaskCount": 63,
                          "callerRunsCount": 64
                        }
                      },
                      "latency": {
                        "readP50Micros": 31,
                        "readP95Micros": 32,
                        "readP99Micros": 33,
                        "writeP50Micros": 34,
                        "writeP95Micros": 35,
                        "writeP99Micros": 36
                      },
                      "bloomFilter": {
                        "hashFunctions": 3,
                        "indexSizeInBytes": 1024,
                        "probabilityOfFalsePositive": 0.01,
                        "requestCount": 38,
                        "refusedCount": 39,
                        "positiveCount": 40,
                        "falsePositiveCount": 41
                      },
                      "wal": {
                        "enabled": false,
                        "appendCount": 0,
                        "appendBytes": 0,
                        "syncCount": 0,
                        "syncFailureCount": 0,
                        "corruptionCount": 0,
                        "truncationCount": 0,
                        "retainedBytes": 0,
                        "segmentCount": 0,
                        "durableLsn": 0,
                        "checkpointLsn": 0,
                        "pendingSyncBytes": 0,
                        "appliedLsn": 0,
                        "checkpointLagLsn": 0,
                        "syncTotalNanos": 0,
                        "syncMaxNanos": 0,
                        "syncBatchBytesTotal": 0,
                        "syncBatchBytesMax": 0,
                        "syncAverageNanos": 0,
                        "syncAverageBatchBytes": 0
                      }
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
