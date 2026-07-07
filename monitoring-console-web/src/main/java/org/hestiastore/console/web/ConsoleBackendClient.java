package org.hestiastore.console.web;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URLEncoder;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.hestiastore.index.segmentindex.monitoring.MonitoredIndex;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexBloomFilterMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexChunkStoreCacheMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexExecutorMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexLatencyMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexMaintenanceMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexRuntimeSnapshot;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexOperationMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexRegistryCacheMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexSegmentMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexSegmentRuntimeMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexSplitMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexWalMetrics;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexWritePathMetrics;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * HTTP client adapter from web UI directly to monitoring-rest-json nodes.
 */
@Service
public class ConsoleBackendClient {

    private static final String CONTENT_TYPE = "Content-Type";
    private static final String APPLICATION_JSON = "application/json";
    private static final String AUTHORIZATION = "Authorization";
    private static final String API_REPORT = "/api/v1/report";
    private static final String API_ACTION_FLUSH = "/api/v1/actions/flush";
    private static final String API_ACTION_COMPACT = "/api/v1/actions/compact";
    private static final String API_CONFIG = "/api/v1/config";
    private static final String FIELD_INDEX_NAME = "indexName";
    private static final String FIELD_CAPTURED_AT = "capturedAt";
    private static final String FIELD_STATE = "state";
    private static final String DEFAULT_STATE = "ERROR";
    private static final String EVENT_NODE_POLL_FAILED = "NODE_POLL_FAILED";
    private static final String POLL_FAILED = "poll failed";
    private static final String OPS_PER_SECOND = "ops/s";
    private static final String BEARER_PREFIX = "Bearer ";
    private static final String FAILED_SEPARATOR = " failed: ";
    private static final int MAX_HISTORY = 200;
    private static final Locale NUMBER_LOCALE = Locale.US;
    private static final char THOUSANDS_SEPARATOR = ',';
    private static final char DECIMAL_SEPARATOR = '.';
    private static final String NUMBER_SEPARATOR_NOTE = "Numeric separators: thousands '"
            + THOUSANDS_SEPARATOR + "', decimal '" + DECIMAL_SEPARATOR + "'.";
    private static final double BYTES_PER_GIB = 1024D * 1024D * 1024D;
    private static final String[] SIZE_UNITS = { "B", "KB", "MB", "GB", "TB",
            "PB" };

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final MonitoringConsoleWebProperties properties;
    private final HttpClient httpClient;
    private final ObjectMapper mapper;
    private final ConcurrentMap<String, MonitoringConsoleWebProperties.NodeEndpoint> nodeById = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ThroughputSample> throughputSamples = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ThroughputSample> counterRateSamples = new ConcurrentHashMap<>();
    private final Deque<ActionRow> actions = new ConcurrentLinkedDeque<>();
    private final Deque<EventRow> events = new ConcurrentLinkedDeque<>();

    /**
     * Creates backend client.
     *
     * @param properties web app properties
     */
    public ConsoleBackendClient(
            final MonitoringConsoleWebProperties properties) {
        this.properties = properties;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3)).build();
        this.mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        for (final MonitoringConsoleWebProperties.NodeEndpoint node : properties
                .nodes()) {
            nodeById.put(node.nodeId(), node);
        }
    }

    /**
     * Fetches dashboard data.
     *
     * @return node rows
     */
    public List<NodeRow> fetchDashboard() {
        final List<NodeRow> rows = new ArrayList<>();
        final long nowNanos = System.nanoTime();
        for (final MonitoringConsoleWebProperties.NodeEndpoint node : properties
                .nodes()) {
            rows.add(fetchNodeDetails(node, nowNanos).node());
        }
        rows.sort(Comparator.comparing(NodeRow::nodeId));
        return rows;
    }

    /**
     * Fetches one node details including aggregated common section and
     * per-index sections.
     *
     * @param nodeId node id
     * @return details when node is configured
     */
    public Optional<NodeDetails> fetchNodeDetails(final String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            return Optional.empty();
        }
        final MonitoringConsoleWebProperties.NodeEndpoint node = nodeById
                .get(nodeId);
        if (node == null) {
            return Optional.empty();
        }
        return Optional.of(fetchNodeDetails(node, System.nanoTime()));
    }

    /**
     * Fetches action history.
     *
     * @return actions
     */
    public List<ActionRow> fetchActions() {
        return List.copyOf(actions);
    }

    /**
     * Fetches event timeline.
     *
     * @return events
     */
    public List<EventRow> fetchEvents() {
        return List.copyOf(events);
    }

    /**
     * Registers node in backend console registry.
     *
     * @param nodeId     node id
     * @param nodeName   display name
     * @param baseUrl    agent base URL
     * @param agentToken optional agent token
     */
    public void registerNode(final String nodeId, final String nodeName,
            final String baseUrl, final String agentToken) {
        throw new IllegalStateException(
                "Direct mode enabled: configure nodes in hestia.console.web.nodes");
    }

    /**
     * Triggers flush/compact operation directly against node management API.
     *
     * @param actionType flush or compact
     * @param nodeId     target node id
     */
    public void triggerAction(final String actionType, final String nodeId) {
        final MonitoringConsoleWebProperties.NodeEndpoint node = nodeById
                .get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("Unknown nodeId: " + nodeId);
        }
        final String path;
        if ("flush".equalsIgnoreCase(actionType)) {
            path = API_ACTION_FLUSH;
        } else if ("compact".equalsIgnoreCase(actionType)) {
            path = API_ACTION_COMPACT;
        } else {
            throw new IllegalArgumentException(
                    "Unsupported action: " + actionType);
        }
        final String requestId = "web-" + UUID.randomUUID();
        final JsonNode response = postJson(node, path,
                Map.of("requestId", requestId));
        final String status = response.path("status").asText("COMPLETED");
        final String message = response.path("message").asText("");
        addAction(new ActionRow(requestId, nodeId, actionType.toUpperCase(),
                status, message, Instant.now().toString()));
        addEvent(new EventRow(Instant.now().toString(), "ACTION_" + status,
                nodeId, actionType.toUpperCase() + " requestId=" + requestId));
    }

    /**
     * Reads runtime/original configuration view for one index on one node.
     *
     * @param nodeId    node id
     * @param indexName index name
     * @return config view when both node and index are available
     */
    public Optional<RuntimeConfigView> fetchRuntimeConfig(final String nodeId,
            final String indexName) {
        if (nodeId == null || nodeId.isBlank() || indexName == null
                || indexName.isBlank()) {
            return Optional.empty();
        }
        final MonitoringConsoleWebProperties.NodeEndpoint node = nodeById
                .get(nodeId);
        if (node == null) {
            return Optional.empty();
        }
        final String encodedIndexName = URLEncoder.encode(indexName.trim(),
                StandardCharsets.UTF_8);
        try {
            final JsonNode config = getJson(node,
                    API_CONFIG + "?" + FIELD_INDEX_NAME + "="
                            + encodedIndexName);
            return Optional.of(new RuntimeConfigView(
                    config.path(FIELD_INDEX_NAME).asText(indexName.trim()),
                    parseConfigMap(config.path("original")),
                    parseConfigMap(config.path("current")),
                    parseStringList(config.path("supportedKeys")),
                    Math.max(0L, config.path("revision").asLong(0L)),
                    asInstantText(config.path(FIELD_CAPTURED_AT))));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("Runtime config poll interrupted: node={} index={}",
                    nodeId, indexName);
            addEvent(new EventRow(Instant.now().toString(),
                    "NODE_CONFIG_POLL_FAILED", nodeId,
                    e.getMessage() == null ? "config poll failed"
                            : e.getMessage()));
            return Optional.empty();
        } catch (final Exception e) {
            logger.info("Runtime config poll failed: node={} index={}", nodeId,
                    indexName);
            addEvent(new EventRow(Instant.now().toString(),
                    "NODE_CONFIG_POLL_FAILED", nodeId,
                    e.getMessage() == null ? "config poll failed"
                            : e.getMessage()));
            return Optional.empty();
        }
    }

    /**
     * Applies or validates runtime config updates for one node/index.
     *
     * @param nodeId    node id
     * @param indexName index name
     * @param values    patch values
     * @param dryRun    when true validates only
     */
    public void patchRuntimeConfig(final String nodeId, final String indexName,
            final Map<String, String> values, final boolean dryRun) {
        final MonitoringConsoleWebProperties.NodeEndpoint node = nodeById
                .get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("Unknown nodeId: " + nodeId);
        }
        if (indexName == null || indexName.isBlank()) {
            throw new IllegalArgumentException("indexName is required");
        }
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException(
                    "No configuration values provided");
        }
        final String encodedIndexName = URLEncoder.encode(indexName.trim(),
                StandardCharsets.UTF_8);
        patchNoContent(node, API_CONFIG + "?indexName=" + encodedIndexName,
                Map.of("values", Map.copyOf(values), "dryRun",
                        Boolean.valueOf(dryRun)));
    }

    private NodeDetails fetchNodeDetails(
            final MonitoringConsoleWebProperties.NodeEndpoint node,
            final long nowNanos) {
        final long startedNanos = System.nanoTime();
        try {
            final JsonNode report = getJson(node, API_REPORT);
            final JsonNode jvm = report.path("jvm");
            final Instant capturedAt = parseInstant(report.path(FIELD_CAPTURED_AT));
            final List<RemoteMonitoredIndex> monitoredIndexes = parseMonitoredIndexes(
                    report.path("indexes"), capturedAt);
            final List<IndexRow> indexRows = toIndexRows(node.nodeId(),
                    monitoredIndexes,
                    asInstantText(report.path(FIELD_CAPTURED_AT)), nowNanos);
            final String resolvedIndexName = monitoredIndexes.isEmpty()
                    ? node.nodeId()
                    : monitoredIndexes.stream().map(MonitoredIndex::indexName)
                            .sorted().collect(Collectors.joining(","));
            final String resolvedNodeName = node.nodeName().isBlank()
                    ? resolvedIndexName
                    : node.nodeName();
            final long readOps = monitoredIndexes.stream()
                    .mapToLong(i -> i.runtimeSnapshot().operations().readOperationCount())
                    .sum();
            final long putOps = monitoredIndexes.stream()
                    .mapToLong(i -> i.runtimeSnapshot().operations().putOperationCount())
                    .sum();
            final long deleteOps = monitoredIndexes.stream()
                    .mapToLong(
                            i -> i.runtimeSnapshot().operations().deleteOperationCount())
                    .sum();
            final long compactRequestCount = monitoredIndexes.stream()
                    .mapToLong(
                            i -> i.runtimeSnapshot().maintenance().compactRequestCount())
                    .sum();
            final long flushRequestCount = monitoredIndexes.stream()
                    .mapToLong(i -> i.runtimeSnapshot().maintenance().flushRequestCount())
                    .sum();
            final long splitScheduleCount = monitoredIndexes.stream()
                    .mapToLong(i -> i.runtimeSnapshot().split().scheduleCount())
                    .sum();
            final long totalOps = Math.max(0L, readOps) + Math.max(0L, putOps)
                    + Math.max(0L, deleteOps);
            final Throughput throughput = computeThroughput(node.nodeId(),
                    totalOps, nowNanos);
            final CounterRate compactRate = computeCounterRate(
                    node.nodeId() + ":compact", compactRequestCount, nowNanos);
            final CounterRate flushRate = computeCounterRate(
                    node.nodeId() + ":flush", flushRequestCount, nowNanos);
            final CounterRate splitRate = computeCounterRate(
                    node.nodeId() + ":split", splitScheduleCount, nowNanos);
            final NodeRow nodeRow = new NodeRow(node.nodeId(), resolvedNodeName,
                    resolvedIndexName, summarizeNodeState(monitoredIndexes),
                    true,
                    !monitoredIndexes.isEmpty() && monitoredIndexes.stream()
                            .allMatch(MonitoredIndex::ready),
                    node.baseUrl(), readOps, putOps, deleteOps,
                    monitoredIndexes.stream()
                            .mapToLong(i -> i.runtimeSnapshot()
                                    .registryCache().hitCount())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToLong(i -> i.runtimeSnapshot()
                                    .registryCache().missCount())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToLong(i -> i.runtimeSnapshot()
                                    .registryCache().loadCount())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToLong(i -> i.runtimeSnapshot()
                                    .registryCache().evictionCount())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.runtimeSnapshot()
                                    .registryCache().size())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.runtimeSnapshot()
                                    .registryCache().limit())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.runtimeSnapshot()
                                    .segments().cacheKeyLimitPerSegment())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.runtimeSnapshot()
                                    .writePath().segmentWriteCacheKeyLimit())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.runtimeSnapshot()
                                    .writePath().segmentWriteCacheKeyLimitDuringMaintenance())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.runtimeSnapshot()
                                    .writePath().indexBufferedWriteKeyLimit())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToInt(
                                    i -> i.runtimeSnapshot().segments().count())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.runtimeSnapshot()
                                    .segments().readyCount())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.runtimeSnapshot()
                                    .segments().maintenanceCount())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.runtimeSnapshot()
                                    .segments().errorCount())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.runtimeSnapshot()
                                    .segments().closedCount())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.runtimeSnapshot()
                                    .segments().unloadedMappedSegmentCount())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToLong(i -> i.runtimeSnapshot()
                                    .segments().totalKeys())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToLong(i -> i.runtimeSnapshot()
                                    .segments().totalCacheKeys())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToLong(i -> i.runtimeSnapshot()
                                    .writePath().totalBufferedWriteKeys())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToLong(i -> i.runtimeSnapshot()
                                    .segments().totalDeltaCacheFiles())
                            .sum(),
                    compactRequestCount, flushRequestCount, splitScheduleCount,
                    compactRate.value(), compactRate.unit(), flushRate.value(),
                    flushRate.unit(), splitRate.value(), splitRate.unit(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.runtimeSnapshot()
                                    .split().inFlightCount())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.runtimeSnapshot()
                                    .maintenance().indexExecutor().queueSize())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.runtimeSnapshot()
                                    .maintenance().indexExecutor().queueCapacity())
                            .sum(),
                    monitoredIndexes.stream().mapToInt(
                            i -> i.runtimeSnapshot().split().executor().queueSize()).sum(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.runtimeSnapshot()
                                    .split().executor().queueCapacity())
                            .sum(),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.runtimeSnapshot().latency().readP50Micros())
                            .max().orElse(0L),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.runtimeSnapshot().latency().readP95Micros())
                            .max().orElse(0L),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.runtimeSnapshot().latency().readP99Micros())
                            .max().orElse(0L),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.runtimeSnapshot().latency().writeP50Micros())
                            .max().orElse(0L),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.runtimeSnapshot().latency().writeP95Micros())
                            .max().orElse(0L),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.runtimeSnapshot().latency().writeP99Micros())
                            .max().orElse(0L),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.runtimeSnapshot()
                                    .bloomFilter().hashFunctions())
                            .max().orElse(0),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.runtimeSnapshot()
                                    .bloomFilter().indexSizeInBytes())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToDouble(i -> i.runtimeSnapshot()
                                    .bloomFilter().probabilityOfFalsePositive())
                            .max().orElse(0D),
                    monitoredIndexes.stream()
                            .mapToLong(i -> i.runtimeSnapshot()
                                    .bloomFilter().requestCount())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToLong(i -> i.runtimeSnapshot()
                                    .bloomFilter().refusedCount())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToLong(i -> i.runtimeSnapshot()
                                    .bloomFilter().positiveCount())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToLong(i -> i.runtimeSnapshot()
                                    .bloomFilter().falsePositiveCount())
                            .sum(),
                    Math.max(0L, jvm.path("heapUsedBytes").asLong(0L)),
                    Math.max(0L, jvm.path("heapCommittedBytes").asLong(0L)),
                    Math.max(0L, jvm.path("heapMaxBytes").asLong(0L)),
                    Math.max(0L, jvm.path("nonHeapUsedBytes").asLong(0L)),
                    Math.max(0L, jvm.path("gcCount").asLong(0L)),
                    Math.max(0L, jvm.path("gcTimeMillis").asLong(0L)),
                    throughput.value(), throughput.unit(),
                    Duration.ofNanos(System.nanoTime() - startedNanos)
                            .toMillis(),
                    asInstantText(report.path(FIELD_CAPTURED_AT)), "");
            return new NodeDetails(nodeRow, indexRows);
        } catch (final HttpTimeoutException e) {
            logger.info("Node poll timed out: {}", node.nodeId());
            addEvent(new EventRow(Instant.now().toString(),
                    EVENT_NODE_POLL_FAILED, node.nodeId(),
                    e.getMessage() == null ? POLL_FAILED : e.getMessage()));
            return new NodeDetails(
                    unavailable(node, e.getMessage(), startedNanos), List.of());
        } catch (final ConnectException e) {
            logger.info("Node poll connect failed: {}", node.nodeId());
            addEvent(new EventRow(Instant.now().toString(),
                    EVENT_NODE_POLL_FAILED, node.nodeId(),
                    e.getMessage() == null ? POLL_FAILED : e.getMessage()));
            return new NodeDetails(
                    unavailable(node, e.getMessage(), startedNanos), List.of());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("Node poll interrupted: {}", node.nodeId());
            addEvent(new EventRow(Instant.now().toString(),
                    EVENT_NODE_POLL_FAILED, node.nodeId(),
                    e.getMessage() == null ? POLL_FAILED : e.getMessage()));
            return new NodeDetails(
                    unavailable(node, e.getMessage(), startedNanos), List.of());
        } catch (final Exception e) {
            logger.warn("Node poll failed: {}", node.nodeId(), e);
            addEvent(new EventRow(Instant.now().toString(),
                    EVENT_NODE_POLL_FAILED, node.nodeId(),
                    e.getMessage() == null ? POLL_FAILED : e.getMessage()));
            return new NodeDetails(
                    unavailable(node, e.getMessage(), startedNanos), List.of());
        }
    }

    private NodeRow unavailable(
            final MonitoringConsoleWebProperties.NodeEndpoint node,
            final String error, final long startedNanos) {
        return new NodeRow(node.nodeId(), node.nodeName(), "", "UNAVAILABLE",
                false, false, node.baseUrl(),
                0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0,
                0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0D, "/s", 0D, "/s", 0D, "/s",
                0, 0, 0, 0, 0,
                0L, 0L, 0L, 0L, 0L, 0L,
                0, 0, 0D,
                0L, 0L, 0L, 0L,
                0L, 0L, 0L, 0L, 0L, 0L, 0D, OPS_PER_SECOND,
                Duration.ofNanos(System.nanoTime() - startedNanos).toMillis(),
                Instant.now().toString(), error == null ? "" : error);
    }

    private JsonNode getJson(
            final MonitoringConsoleWebProperties.NodeEndpoint node,
            final String path) throws IOException, InterruptedException {
        final HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(node.baseUrl() + path))
                .timeout(Duration.ofSeconds(4)).GET();
        if (!node.agentToken().isEmpty()) {
            builder.header(AUTHORIZATION, BEARER_PREFIX + node.agentToken());
        }
        final HttpResponse<String> response = httpClient.send(builder.build(),
                HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 200 && response.statusCode() < 300) {
            return mapper.readTree(response.body());
        }
        throw new IllegalStateException(
                "GET " + path + FAILED_SEPARATOR + response.statusCode());
    }

    private JsonNode postJson(
            final MonitoringConsoleWebProperties.NodeEndpoint node,
            final String path, final Map<String, Object> body) {
        try {
            final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(node.baseUrl() + path))
                    .timeout(Duration.ofSeconds(5))
                    .header(CONTENT_TYPE, APPLICATION_JSON);
            if (!node.agentToken().isEmpty()) {
                requestBuilder.header(AUTHORIZATION,
                        BEARER_PREFIX + node.agentToken());
            }
            final HttpRequest request = requestBuilder
                    .POST(HttpRequest.BodyPublishers
                            .ofString(mapper.writeValueAsString(body)))
                    .build();
            final HttpResponse<String> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                if (response.body() == null || response.body().isBlank()) {
                    return mapper.createObjectNode();
                }
                return mapper.readTree(response.body());
            }
            throw new IllegalStateException(
                    formatHttpError("POST", path, response));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("POST " + path + " failed", e);
        } catch (final Exception e) {
            throw new IllegalStateException("POST " + path + " failed", e);
        }
    }

    private void patchNoContent(
            final MonitoringConsoleWebProperties.NodeEndpoint node,
            final String path, final Map<String, Object> body) {
        try {
            final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(node.baseUrl() + path))
                    .timeout(Duration.ofSeconds(5))
                    .header(CONTENT_TYPE, APPLICATION_JSON);
            if (!node.agentToken().isEmpty()) {
                requestBuilder.header(AUTHORIZATION,
                        BEARER_PREFIX + node.agentToken());
            }
            final HttpRequest request = requestBuilder
                    .method("PATCH",
                            HttpRequest.BodyPublishers
                                    .ofString(mapper.writeValueAsString(body)))
                    .build();
            final HttpResponse<String> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return;
            }
            throw new IllegalStateException(
                    formatHttpError("PATCH", path, response));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("PATCH " + path + " failed", e);
        } catch (final Exception e) {
            throw new IllegalStateException("PATCH " + path + " failed", e);
        }
    }

    private String formatHttpError(final String method, final String path,
            final HttpResponse<String> response) {
        final String body = response.body() == null ? ""
                : response.body().trim();
        if (body.isBlank()) {
            return method + " " + path + FAILED_SEPARATOR
                    + response.statusCode();
        }
        try {
            final JsonNode json = mapper.readTree(body);
            final String code = json.path("code").asText("");
            final String message = json.path("message").asText("");
            if (!code.isBlank() || !message.isBlank()) {
                return method + " " + path + FAILED_SEPARATOR
                        + response.statusCode() + " " + code + " " + message;
            }
        } catch (final Exception e) {
            logger.trace("Response body is not a JSON error payload.", e);
        }
        return method + " " + path + FAILED_SEPARATOR + response.statusCode()
                + " body=" + body;
    }

    private String asInstantText(final JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return "";
        }
        if (node.isTextual()) {
            return node.asText();
        }
        if (node.isNumber()) {
            return Instant.ofEpochSecond(node.asLong()).toString();
        }
        return node.toString();
    }

    private Instant parseInstant(final JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return Instant.EPOCH;
        }
        if (node.isNumber()) {
            return Instant.ofEpochSecond(node.asLong());
        }
        if (node.isTextual()) {
            try {
                return Instant.parse(node.asText());
            } catch (final DateTimeParseException ex) {
                return Instant.EPOCH;
            }
        }
        return Instant.EPOCH;
    }

    private List<IndexRow> toIndexRows(final String nodeId,
            final List<RemoteMonitoredIndex> monitoredIndexes,
            final String capturedAt, final long nowNanos) {
        final List<IndexRow> rows = new ArrayList<>();
        for (final RemoteMonitoredIndex monitoredIndex : monitoredIndexes) {
            final SegmentIndexRuntimeSnapshot snapshot = monitoredIndex
                    .runtimeSnapshot();
            final long totalOps = nonNegativeLong(
                    snapshot.operations().readOperationCount())
                    + nonNegativeLong(snapshot.operations().putOperationCount())
                    + nonNegativeLong(snapshot.operations().deleteOperationCount());
            final Throughput throughput = computeThroughput(
                    nodeId + ":" + monitoredIndex.indexName(), totalOps,
                    nowNanos);
            final CounterRate compactRate = computeCounterRate(
                    nodeId + ":" + monitoredIndex.indexName() + ":compact",
                    snapshot.maintenance().compactRequestCount(), nowNanos);
            final CounterRate flushRate = computeCounterRate(
                    nodeId + ":" + monitoredIndex.indexName() + ":flush",
                    snapshot.maintenance().flushRequestCount(), nowNanos);
            final CounterRate splitRate = computeCounterRate(
                    nodeId + ":" + monitoredIndex.indexName() + ":split",
                    snapshot.split().scheduleCount(), nowNanos);
            rows.add(new IndexRow(monitoredIndex.indexName(),
                    monitoredIndex.state().name(), monitoredIndex.ready(),
                    snapshot.operations().readOperationCount(),
                    snapshot.operations().putOperationCount(),
                    snapshot.operations().deleteOperationCount(),
                    snapshot.registryCache().hitCount(),
                    snapshot.registryCache().missCount(),
                    snapshot.registryCache().loadCount(),
                    snapshot.registryCache().evictionCount(),
                    snapshot.registryCache().size(),
                    snapshot.registryCache().limit(),
                    snapshot.segments().cacheKeyLimitPerSegment(),
                    snapshot.writePath().segmentWriteCacheKeyLimit(),
                    snapshot.writePath().segmentWriteCacheKeyLimitDuringMaintenance(),
                    snapshot.writePath().indexBufferedWriteKeyLimit(),
                    snapshot.segments().count(), snapshot.segments().readyCount(),
                    snapshot.segments().maintenanceCount(),
                    snapshot.segments().errorCount(),
                    snapshot.segments().closedCount(),
                    snapshot.segments().unloadedMappedSegmentCount(),
                    snapshot.segments().totalKeys(),
                    snapshot.segments().totalCacheKeys(),
                    snapshot.writePath().totalBufferedWriteKeys(),
                    snapshot.segments().totalDeltaCacheFiles(),
                    snapshot.latency().readP50Micros(),
                    snapshot.latency().readP95Micros(),
                    snapshot.latency().readP99Micros(),
                    snapshot.latency().writeP50Micros(),
                    snapshot.latency().writeP95Micros(),
                    snapshot.latency().writeP99Micros(),
                    snapshot.bloomFilter().hashFunctions(),
                    snapshot.bloomFilter().indexSizeInBytes(),
                    snapshot.bloomFilter().probabilityOfFalsePositive(),
                    snapshot.bloomFilter().requestCount(),
                    snapshot.bloomFilter().refusedCount(),
                    snapshot.bloomFilter().positiveCount(),
                    snapshot.bloomFilter().falsePositiveCount(),
                    snapshot.maintenance().flushRequestCount(),
                    snapshot.maintenance().compactRequestCount(),
                    snapshot.split().scheduleCount(),
                    snapshot.split().inFlightCount(),
                    snapshot.maintenance().indexExecutor().queueSize(),
                    snapshot.maintenance().indexExecutor().queueCapacity(),
                    snapshot.split().executor().queueSize(),
                    snapshot.split().executor().queueCapacity(),
                    throughput.value(),
                    throughput.unit(), compactRate.value(), compactRate.unit(),
                    flushRate.value(), flushRate.unit(), splitRate.value(),
                    splitRate.unit(), capturedAt,
                    toSegmentRows(snapshot.segments().runtimeMetrics())));
        }
        rows.sort(Comparator.comparing(IndexRow::indexName));
        return List.copyOf(rows);
    }

    private List<RemoteMonitoredIndex> parseMonitoredIndexes(
            final JsonNode indexesNode, final Instant capturedAt) {
        if (indexesNode == null || !indexesNode.isArray()) {
            return List.of();
        }
        final List<RemoteMonitoredIndex> parsed = new ArrayList<>();
        for (final JsonNode indexNode : indexesNode) {
            parsed.add(new RemoteMonitoredIndex(
                    indexNode.path(FIELD_INDEX_NAME).asText("unknown-index"),
                    parseState(
                            indexNode.path(FIELD_STATE).asText(DEFAULT_STATE)),
                    parseRuntimeSnapshot(indexNode, capturedAt)));
        }
        return List.copyOf(parsed);
    }

    private SegmentIndexRuntimeSnapshot parseRuntimeSnapshot(
            final JsonNode indexNode, final Instant capturedAt) {
        final JsonNode operations = indexNode.path("operations");
        final JsonNode registryCache = indexNode.path("registryCache");
        final JsonNode chunkStoreCache = indexNode.path("chunkStoreCache");
        final JsonNode segments = indexNode.path("segments");
        final JsonNode writePath = indexNode.path("writePath");
        final JsonNode maintenance = indexNode.path("maintenance");
        final JsonNode split = indexNode.path("split");
        final JsonNode latency = indexNode.path("latency");
        final JsonNode bloomFilter = indexNode.path("bloomFilter");
        final JsonNode wal = indexNode.path("wal");
        final String indexName = indexNode.path(FIELD_INDEX_NAME).asText(
                "unknown-index");
        final SegmentIndexState state = parseState(
                indexNode.path(FIELD_STATE).asText(DEFAULT_STATE));
        return new SegmentIndexRuntimeSnapshot(
                indexName,
                state,
                capturedAt,
                new SegmentIndexOperationMetrics(
                        nonNegativeLong(operations.path("readOperationCount")
                                .asLong(0L)),
                        nonNegativeLong(operations.path("putOperationCount")
                                .asLong(0L)),
                        nonNegativeLong(operations.path("deleteOperationCount")
                                .asLong(0L))),
                new SegmentIndexRegistryCacheMetrics(
                        nonNegativeLong(registryCache.path("hitCount")
                                .asLong(0L)),
                        nonNegativeLong(registryCache.path("missCount")
                                .asLong(0L)),
                        nonNegativeLong(registryCache.path("loadCount")
                                .asLong(0L)),
                        nonNegativeLong(registryCache.path("evictionCount")
                                .asLong(0L)),
                        nonNegativeInt(registryCache.path("size").asInt(0)),
                        nonNegativeInt(registryCache.path("limit").asInt(0))),
                new SegmentIndexChunkStoreCacheMetrics(
                        nonNegativeInt(chunkStoreCache.path("pageLimit")
                                .asInt(0)),
                        nonNegativeInt(chunkStoreCache.path("pageCount")
                                .asInt(0)),
                        nonNegativeLong(chunkStoreCache.path("entryCount")
                                .asLong(0L)),
                        nonNegativeLong(chunkStoreCache.path("hitCount")
                                .asLong(0L)),
                        nonNegativeLong(chunkStoreCache.path("missCount")
                                .asLong(0L)),
                        nonNegativeLong(chunkStoreCache.path("loadCount")
                                .asLong(0L)),
                        nonNegativeLong(chunkStoreCache.path("evictionCount")
                                .asLong(0L)),
                        nonNegativeLong(chunkStoreCache.path("invalidationCount")
                                .asLong(0L))),
                new SegmentIndexSegmentMetrics(
                        nonNegativeInt(segments.path("cacheKeyLimitPerSegment")
                                .asInt(0)),
                        nonNegativeInt(segments.path("count").asInt(0)),
                        nonNegativeInt(segments.path("readyCount").asInt(0)),
                        nonNegativeInt(segments.path("maintenanceCount")
                                .asInt(0)),
                        nonNegativeInt(segments.path("errorCount").asInt(0)),
                        nonNegativeInt(segments.path("closedCount").asInt(0)),
                        nonNegativeInt(segments
                                .path("unloadedMappedSegmentCount").asInt(0)),
                        nonNegativeLong(segments.path("totalKeys").asLong(0L)),
                        nonNegativeLong(segments.path("totalCacheKeys")
                                .asLong(0L)),
                        nonNegativeLong(segments.path("totalDeltaCacheFiles")
                                .asLong(0L)),
                        parseSegmentRuntimeSnapshots(
                                segments.path("runtimeMetrics"))),
                new SegmentIndexWritePathMetrics(
                        nonNegativeInt(writePath
                                .path("segmentWriteCacheKeyLimit").asInt(0)),
                        nonNegativeInt(writePath
                                .path("segmentWriteCacheKeyLimitDuringMaintenance")
                                .asInt(0)),
                        nonNegativeInt(writePath
                                .path("indexBufferedWriteKeyLimit").asInt(0)),
                        nonNegativeLong(writePath
                                .path("totalBufferedWriteKeys").asLong(0L))),
                new SegmentIndexMaintenanceMetrics(
                        nonNegativeLong(maintenance.path("compactRequestCount")
                                .asLong(0L)),
                        nonNegativeLong(maintenance.path("flushRequestCount")
                                .asLong(0L)),
                        nonNegativeLong(maintenance
                                .path("flushAcceptedToReadyP95Micros")
                                .asLong(0L)),
                        nonNegativeLong(maintenance
                                .path("compactAcceptedToReadyP95Micros")
                                .asLong(0L)),
                        nonNegativeLong(maintenance.path("flushBusyRetryCount")
                                .asLong(0L)),
                        nonNegativeLong(maintenance
                                .path("compactBusyRetryCount").asLong(0L)),
                        parseExecutor(maintenance.path("indexExecutor")),
                        parseExecutor(
                                maintenance.path("stableSegmentExecutor"))),
                new SegmentIndexSplitMetrics(
                        nonNegativeLong(split.path("scheduleCount")
                                .asLong(0L)),
                        nonNegativeInt(split.path("inFlightCount").asInt(0)),
                        nonNegativeInt(split.path("blockedCount").asInt(0)),
                        nonNegativeLong(split.path("taskStartDelayP95Micros")
                                .asLong(0L)),
                        nonNegativeLong(split.path("taskRunLatencyP95Micros")
                                .asLong(0L)),
                        parseExecutor(split.path("executor"))),
                new SegmentIndexLatencyMetrics(
                        nonNegativeLong(latency.path("readP50Micros")
                                .asLong(0L)),
                        nonNegativeLong(latency.path("readP95Micros")
                                .asLong(0L)),
                        nonNegativeLong(latency.path("readP99Micros")
                                .asLong(0L)),
                        nonNegativeLong(latency.path("writeP50Micros")
                                .asLong(0L)),
                        nonNegativeLong(latency.path("writeP95Micros")
                                .asLong(0L)),
                        nonNegativeLong(latency.path("writeP99Micros")
                                .asLong(0L))),
                new SegmentIndexBloomFilterMetrics(
                        nonNegativeInt(bloomFilter.path("hashFunctions")
                                .asInt(0)),
                        nonNegativeInt(bloomFilter.path("indexSizeInBytes")
                                .asInt(0)),
                        Math.max(0D, bloomFilter
                                .path("probabilityOfFalsePositive")
                                .asDouble(0D)),
                        nonNegativeLong(bloomFilter.path("requestCount")
                                .asLong(0L)),
                        nonNegativeLong(bloomFilter.path("refusedCount")
                                .asLong(0L)),
                        nonNegativeLong(bloomFilter.path("positiveCount")
                                .asLong(0L)),
                        nonNegativeLong(bloomFilter.path("falsePositiveCount")
                                .asLong(0L))),
                new SegmentIndexWalMetrics(wal.path("enabled").asBoolean(false),
                        nonNegativeLong(wal.path("appendCount").asLong(0L)),
                        nonNegativeLong(wal.path("appendBytes").asLong(0L)),
                        nonNegativeLong(wal.path("syncCount").asLong(0L)),
                        nonNegativeLong(wal.path("syncFailureCount")
                                .asLong(0L)),
                        nonNegativeLong(wal.path("corruptionCount")
                                .asLong(0L)),
                        nonNegativeLong(wal.path("truncationCount")
                                .asLong(0L)),
                        nonNegativeLong(wal.path("retainedBytes").asLong(0L)),
                        nonNegativeInt(wal.path("segmentCount").asInt(0)),
                        nonNegativeLong(wal.path("durableLsn").asLong(0L)),
                        nonNegativeLong(wal.path("checkpointLsn").asLong(0L)),
                        nonNegativeLong(wal.path("pendingSyncBytes")
                                .asLong(0L)),
                        nonNegativeLong(wal.path("appliedLsn").asLong(0L)),
                        nonNegativeLong(wal.path("syncTotalNanos")
                                .asLong(0L)),
                        nonNegativeLong(wal.path("syncMaxNanos").asLong(0L)),
                        nonNegativeLong(wal.path("syncBatchBytesTotal")
                                .asLong(0L)),
                        nonNegativeLong(wal.path("syncBatchBytesMax")
                                .asLong(0L))));
    }

    private SegmentIndexExecutorMetrics parseExecutor(final JsonNode node) {
        return new SegmentIndexExecutorMetrics(
                nonNegativeInt(node.path("activeThreadCount").asInt(0)),
                nonNegativeInt(node.path("queueSize").asInt(0)),
                nonNegativeInt(node.path("queueCapacity").asInt(0)),
                nonNegativeLong(node.path("completedTaskCount").asLong(0L)),
                nonNegativeLong(node.path("rejectedTaskCount").asLong(0L)),
                nonNegativeLong(node.path("callerRunsCount").asLong(0L)));
    }

    private List<SegmentIndexSegmentRuntimeMetrics> parseSegmentRuntimeSnapshots(
            final JsonNode snapshotsNode) {
        if (snapshotsNode == null || !snapshotsNode.isArray()) {
            return List.of();
        }
        final List<SegmentIndexSegmentRuntimeMetrics> parsed =
                new ArrayList<>();
        for (final JsonNode snapshotNode : snapshotsNode) {
            parsed.add(new SegmentIndexSegmentRuntimeMetrics(
                    snapshotNode.path("segmentId").asText("unknown-segment"),
                    parseSegmentState(snapshotNode.path(FIELD_STATE)
                            .asText(DEFAULT_STATE)),
                    nonNegativeLong(snapshotNode
                            .path("numberOfKeysInDeltaCache").asLong(0L)),
                    nonNegativeLong(snapshotNode.path("numberOfKeysInSegment")
                            .asLong(0L)),
                    nonNegativeLong(snapshotNode
                            .path("numberOfKeysInScarceIndex").asLong(0L)),
                    nonNegativeLong(snapshotNode
                            .path("numberOfKeysInSegmentCache").asLong(0L)),
                    nonNegativeInt(snapshotNode.path("numberOfKeysInWriteCache")
                            .asInt(0)),
                    nonNegativeInt(snapshotNode.path("numberOfDeltaCacheFiles")
                            .asInt(0)),
                    nonNegativeLong(snapshotNode.path("compactRequestCount")
                            .asLong(0L)),
                    nonNegativeLong(
                            snapshotNode.path("flushRequestCount").asLong(0L)),
                    nonNegativeLong(snapshotNode.path("bloomFilterRequestCount")
                            .asLong(0L)),
                    nonNegativeLong(snapshotNode.path("bloomFilterRefusedCount")
                            .asLong(0L)),
                    nonNegativeLong(snapshotNode
                            .path("bloomFilterPositiveCount").asLong(0L)),
                    nonNegativeLong(
                            snapshotNode.path("bloomFilterFalsePositiveCount")
                                    .asLong(0L))));
        }
        return List.copyOf(parsed);
    }

    private SegmentState parseSegmentState(final String state) {
        if (state == null) {
            return SegmentState.ERROR;
        }
        try {
            return SegmentState.valueOf(state.trim().toUpperCase(Locale.ROOT));
        } catch (final IllegalArgumentException e) {
            return SegmentState.ERROR;
        }
    }

    private List<SegmentRow> toSegmentRows(
            final List<SegmentIndexSegmentRuntimeMetrics> snapshots) {
        if (snapshots == null || snapshots.isEmpty()) {
            return List.of();
        }
        final List<SegmentRow> rows = new ArrayList<>(snapshots.size());
        for (final SegmentIndexSegmentRuntimeMetrics snapshot : snapshots) {
            rows.add(new SegmentRow(snapshot.segmentId(),
                    snapshot.state().name(),
                    snapshot.numberOfKeysInDeltaCache(),
                    snapshot.numberOfKeysInSegment(),
                    snapshot.numberOfKeysInScarceIndex(),
                    snapshot.numberOfKeysInSegmentCache(),
                    snapshot.numberOfKeysInWriteCache(),
                    snapshot.numberOfDeltaCacheFiles(),
                    snapshot.compactRequestCount(),
                    snapshot.flushRequestCount(),
                    snapshot.bloomFilterRequestCount(),
                    snapshot.bloomFilterRefusedCount(),
                    snapshot.bloomFilterPositiveCount(),
                    snapshot.bloomFilterFalsePositiveCount()));
        }
        rows.sort(Comparator.comparing(SegmentRow::segmentId));
        return List.copyOf(rows);
    }

    private SegmentIndexState parseState(final String state) {
        if (state == null) {
            return SegmentIndexState.ERROR;
        }
        try {
            return SegmentIndexState
                    .valueOf(state.trim().toUpperCase(Locale.ROOT));
        } catch (final IllegalArgumentException e) {
            return SegmentIndexState.ERROR;
        }
    }

    private String summarizeNodeState(
            final List<RemoteMonitoredIndex> indexes) {
        if (indexes.isEmpty()) {
            return "UNAVAILABLE";
        }
        if (indexes.stream().allMatch(MonitoredIndex::ready)) {
            return SegmentIndexState.READY.name();
        }
        if (indexes.stream()
                .anyMatch(i -> i.state() == SegmentIndexState.ERROR)) {
            return SegmentIndexState.ERROR.name();
        }
        if (indexes.stream()
                .anyMatch(i -> i.state() == SegmentIndexState.OPENING)) {
            return SegmentIndexState.OPENING.name();
        }
        return "DEGRADED";
    }

    private long nonNegativeLong(final long value) {
        return Math.max(0L, value);
    }

    private int nonNegativeInt(final int value) {
        return Math.max(0, value);
    }

    private Map<String, Integer> parseConfigMap(final JsonNode node) {
        if (node == null || !node.isObject()) {
            return Map.of();
        }
        final Map<String, Integer> values = new LinkedHashMap<>();
        final TreeSet<String> sortedKeys = new TreeSet<>();
        node.fieldNames().forEachRemaining(sortedKeys::add);
        for (final String key : sortedKeys) {
            final JsonNode valueNode = node.get(key);
            if (valueNode == null || !valueNode.isNumber()) {
                continue;
            }
            values.put(key, Integer.valueOf(valueNode.asInt()));
        }
        return Map.copyOf(values);
    }

    private List<String> parseStringList(final JsonNode node) {
        if (node == null || !node.isArray()) {
            return List.of();
        }
        final List<String> values = new ArrayList<>();
        for (final JsonNode item : node) {
            if (!item.isTextual()) {
                continue;
            }
            final String text = item.asText("").trim();
            if (!text.isEmpty()) {
                values.add(text);
            }
        }
        return List.copyOf(values);
    }

    private void addAction(final ActionRow row) {
        actions.addFirst(row);
        while (actions.size() > MAX_HISTORY) {
            actions.removeLast();
        }
    }

    private void addEvent(final EventRow row) {
        events.addFirst(row);
        while (events.size() > MAX_HISTORY) {
            events.removeLast();
        }
    }

    private static String formatWholeNumberValue(final long value) {
        return String.format(NUMBER_LOCALE, "%,d", value);
    }

    private static String formatBytes(final long bytes) {
        if (bytes <= 0L) {
            return "0 B";
        }
        double value = bytes;
        int unitIndex = 0;
        while (value >= 1024D && unitIndex < SIZE_UNITS.length - 1) {
            value /= 1024D;
            unitIndex++;
        }
        final int fractionDigits;
        if (value >= 100D) {
            fractionDigits = 0;
        } else if (value >= 10D) {
            fractionDigits = 1;
        } else {
            fractionDigits = 2;
        }
        final String compact = formatDecimal(value, 0, fractionDigits);
        return compact + " " + SIZE_UNITS[unitIndex];
    }

    private static String formatGigabytes(final long bytes) {
        if (bytes <= 0L) {
            return "0 GB";
        }
        return formatDecimal(bytes / BYTES_PER_GIB, 0, 2) + " GB";
    }

    private static String format4Significant(final double value) {
        if (value <= 0D || Double.isNaN(value) || Double.isInfinite(value)) {
            return "0";
        }
        final BigDecimal rounded = BigDecimal.valueOf(value)
                .round(new MathContext(4, RoundingMode.HALF_UP))
                .stripTrailingZeros();
        final int maxFraction = Math.max(0, Math.min(12, rounded.scale()));
        return formatDecimal(rounded.doubleValue(), 0, maxFraction);
    }

    private static String formatDecimal(final double value,
            final int minFractionDigits, final int maxFractionDigits) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return "0";
        }
        final DecimalFormatSymbols symbols = DecimalFormatSymbols
                .getInstance(NUMBER_LOCALE);
        final DecimalFormat format = new DecimalFormat("#,##0.############",
                symbols);
        format.setGroupingUsed(true);
        format.setMinimumFractionDigits(Math.max(0, minFractionDigits));
        format.setMaximumFractionDigits(
                Math.max(minFractionDigits, maxFractionDigits));
        return format.format(value);
    }

    /**
     * Dashboard row model.
     */
    public record NodeRow(String nodeId, String nodeName, String indexName,
            String state, boolean reachable, boolean ready, String baseUrl,
            long readOps, long putOps, long deleteOps, long cacheHitCount,
            long cacheMissCount, long cacheLoadCount, long cacheEvictionCount,
            int cacheSize, int cacheLimit, int segmentCacheKeyLimitPerSegment,
            int segmentWriteCacheKeyLimit,
            int segmentWriteCacheKeyLimitDuringMaintenance,
            int indexBufferedWriteKeyLimit,
            int segmentCount, int segmentReadyCount,
            int segmentMaintenanceCount, int segmentErrorCount,
            int segmentClosedCount, int unloadedMappedSegmentCount,
            long totalSegmentKeys,
            long totalSegmentCacheKeys, long totalBufferedWriteKeys,
            long totalDeltaCacheFiles, long compactRequestCount,
            long flushRequestCount, long splitScheduleCount,
            double compactRateValue, String compactRateUnit,
            double flushRateValue, String flushRateUnit, double splitRateValue,
            String splitRateUnit, int splitInFlightCount,
            int maintenanceQueueSize, int maintenanceQueueCapacity,
            int splitQueueSize, int splitQueueCapacity,
            long readLatencyP50Micros, long readLatencyP95Micros,
            long readLatencyP99Micros, long writeLatencyP50Micros,
            long writeLatencyP95Micros, long writeLatencyP99Micros,
            int bloomFilterHashFunctions, int bloomFilterIndexSizeInBytes,
            double bloomFilterProbabilityOfFalsePositive,
            long bloomFilterRequestCount, long bloomFilterRefusedCount,
            long bloomFilterPositiveCount, long bloomFilterFalsePositiveCount,
            long jvmHeapUsedBytes, long jvmHeapCommittedBytes,
            long jvmHeapMaxBytes, long jvmNonHeapUsedBytes, long jvmGcCount,
            long jvmGcTimeMillis, double currentThroughputValue,
            String currentThroughputUnit, long latencyMs, String capturedAt,
            String error) {
        /**
         * Total operations count.
         *
         * @return sum of read/put/delete
         */
        public long totalOps() {
            return readOps + putOps + deleteOps;
        }

        /**
         * Formats integer value using consistent grouping separators.
         *
         * @param value integer value
         * @return formatted value (for example 12,345)
         */
        public String formatWholeNumber(final long value) {
            return formatWholeNumberValue(value);
        }

        /**
         * Formats decimal value in compact style with grouping separators.
         *
         * @param value decimal value
         * @return formatted value
         */
        public String formatCompactDecimal(final double value) {
            return format4Significant(value);
        }

        /**
         * Returns a note that explains numeric separators used in UI.
         *
         * @return separator note
         */
        public String numberSeparatorsNote() {
            return NUMBER_SEPARATOR_NOTE;
        }

        /**
         * Formats integer value using consistent grouping separators.
         *
         * @param value integer value
         * @return formatted value (for example 12,345)
         */
        public static String formatWholeNumberValue(final long value) {
            return ConsoleBackendClient.formatWholeNumberValue(value);
        }

        /**
         * Cache hit ratio [0..1].
         *
         * @return hit ratio
         */
        public double cacheHitRatio() {
            final long total = cacheHitCount + cacheMissCount;
            if (total <= 0L) {
                return 0D;
            }
            return ((double) cacheHitCount) / ((double) total);
        }

        /**
         * Cache hit ratio in percentage with one decimal.
         *
         * @return percentage display
         */
        public String cacheHitRatioPercentDisplay() {
            return formatDecimal(cacheHitRatio() * 100D, 1, 1);
        }

        /**
         * Heap used in compact human-readable units.
         *
         * @return heap usage, e.g. "8.23 GB" or "737 MB"
         */
        public String heapUsedHumanReadable() {
            return formatBytes(jvmHeapUsedBytes);
        }

        /**
         * Heap committed in gigabytes.
         *
         * @return committed heap in GB
         */
        public String heapCommittedGigabytesDisplay() {
            return formatGigabytes(jvmHeapCommittedBytes);
        }

        /**
         * Heap used in gigabytes.
         *
         * @return used heap in GB
         */
        public String heapUsedGigabytesDisplay() {
            return formatGigabytes(jvmHeapUsedBytes);
        }

        /**
         * Heap Xmx in gigabytes.
         *
         * @return heap max in GB
         */
        public String heapMaxGigabytesDisplay() {
            return formatGigabytes(jvmHeapMaxBytes);
        }

        /**
         * Heap free bytes estimated from committed minus used.
         *
         * @return free heap bytes
         */
        public long heapFreeBytes() {
            return Math.max(0L, jvmHeapCommittedBytes - jvmHeapUsedBytes);
        }

        /**
         * Heap free in gigabytes.
         *
         * @return free heap in GB
         */
        public String heapFreeGigabytesDisplay() {
            return formatGigabytes(heapFreeBytes());
        }

        /**
         * Heap free percentage from committed heap.
         *
         * @return free heap percent
         */
        public long heapFreePercent() {
            if (jvmHeapCommittedBytes <= 0L) {
                return 0L;
            }
            return Math.round(heapFreeBytes() * 100D / jvmHeapCommittedBytes);
        }

        /**
         * Bloom index size in compact human-readable units.
         *
         * @return bloom index size, e.g. "10 MB"
         */
        public String bloomIndexSizeHumanReadable() {
            return formatBytes(bloomFilterIndexSizeInBytes);
        }

        /**
         * Estimated max cache keys across all segments.
         *
         * @return estimated key limit
         */
        public long estimatedSegmentCacheKeyLimit() {
            return Math.max(0L, (long) segmentCacheKeyLimitPerSegment
                    * Math.max(0, segmentCount));
        }

        /**
         * Segment cache pressure percentage based on estimated key limit.
         *
         * @return percentage in range [0..100]
         */
        public long segmentCachePressurePercent() {
            final long limit = estimatedSegmentCacheKeyLimit();
            if (limit <= 0L) {
                return 0L;
            }
            return Math.round(
                    Math.max(0L, totalSegmentCacheKeys) * 100D / limit);
        }

        /**
         * Registry cache fill percentage.
         *
         * @return percentage in range [0..100]
         */
        public long registryCacheFillPercent() {
            if (cacheLimit <= 0) {
                return 0L;
            }
            return Math.round(Math.max(0, cacheSize) * 100D / cacheLimit);
        }

        /**
         * Registry cache hit ratio as percentage.
         *
         * @return percentage in range [0..100]
         */
        public long registryCacheHitRatioPercent() {
            return Math.round(cacheHitRatio() * 100D);
        }

        /**
         * Throughput display with compact precision.
         *
         * @return throughput value with unit, e.g. "12.34 ops/s"
         */
        public String currentThroughputDisplay() {
            return format4Significant(currentThroughputValue) + " "
                    + currentThroughputUnit;
        }

        /**
         * Current compact requests rate display.
         *
         * @return compact rate with auto-selected unit
         */
        public String compactRateDisplay() {
            return format4Significant(compactRateValue) + " " + compactRateUnit;
        }

        /**
         * Current flush requests rate display.
         *
         * @return flush rate with auto-selected unit
         */
        public String flushRateDisplay() {
            return format4Significant(flushRateValue) + " " + flushRateUnit;
        }

        /**
         * Current split schedules rate display.
         *
         * @return split rate with auto-selected unit
         */
        public String splitRateDisplay() {
            return format4Significant(splitRateValue) + " " + splitRateUnit;
        }

        /**
         * Average delta cache files per segment.
         *
         * @return average delta files/segment as compact decimal
         */
        public String averageDeltaCacheFilesPerSegmentDisplay() {
            if (segmentCount <= 0) {
                return "0";
            }
            final double average = ((double) totalDeltaCacheFiles)
                    / ((double) segmentCount);
            return format4Significant(average);
        }

    }

    private record RemoteMonitoredIndex(String indexName,
            SegmentIndexState state,
            SegmentIndexRuntimeSnapshot runtimeSnapshot)
            implements MonitoredIndex {
    }

    private Throughput computeThroughput(final String nodeId,
            final long totalOps, final long nowNanos) {
        if (nodeId == null || nodeId.isBlank()) {
            return new Throughput(0D, OPS_PER_SECOND);
        }
        final ThroughputSample previous = throughputSamples.put(nodeId,
                new ThroughputSample(totalOps, nowNanos));
        if (previous == null) {
            return new Throughput(0D, OPS_PER_SECOND);
        }
        final long deltaOps = totalOps - previous.totalOps();
        final long deltaNanos = nowNanos - previous.atNanos();
        if (deltaOps <= 0L || deltaNanos <= 0L) {
            return new Throughput(0D, OPS_PER_SECOND);
        }
        double value = deltaOps * 1_000_000_000D / deltaNanos;
        String unit = OPS_PER_SECOND;
        if (value >= 10_000D) {
            value /= 1_000D;
            unit = "ops/ms";
        }
        if (value >= 10_000D) {
            value /= 1_000D;
            unit = "ops/us";
        }
        if (value >= 10_000D) {
            value /= 1_000D;
            unit = "ops/ns";
        }
        return new Throughput(value, unit);
    }

    private record ThroughputSample(long totalOps, long atNanos) {
    }

    private record Throughput(double value, String unit) {
    }

    private CounterRate computeCounterRate(final String sampleKey,
            final long totalCount, final long nowNanos) {
        if (sampleKey == null || sampleKey.isBlank()) {
            return new CounterRate(0D, "/s");
        }
        final ThroughputSample previous = counterRateSamples.put(sampleKey,
                new ThroughputSample(totalCount, nowNanos));
        if (previous == null) {
            return new CounterRate(0D, "/s");
        }
        final long deltaCount = totalCount - previous.totalOps();
        final long deltaNanos = nowNanos - previous.atNanos();
        if (deltaCount <= 0L || deltaNanos <= 0L) {
            return new CounterRate(0D, "/s");
        }
        final double perSecond = deltaCount * 1_000_000_000D / deltaNanos;
        if (perSecond >= 1D) {
            return new CounterRate(perSecond, "/s");
        }
        final double perMinute = perSecond * 60D;
        if (perMinute >= 1D) {
            return new CounterRate(perMinute, "/min");
        }
        return new CounterRate(perMinute * 60D, "/h");
    }

    private record CounterRate(double value, String unit) {
    }

    /**
     * Node detail model with one common section and per-index sections.
     */
    public record NodeDetails(NodeRow node, List<IndexRow> indexes) {
    }

    /**
     * Per-index section model shown in node detail.
     */
    public record IndexRow(String indexName, String state, boolean ready,
            long readOps, long putOps, long deleteOps, long cacheHitCount,
            long cacheMissCount, long cacheLoadCount, long cacheEvictionCount,
            int cacheSize, int cacheLimit, int segmentCacheKeyLimitPerSegment,
            int segmentWriteCacheKeyLimit,
            int segmentWriteCacheKeyLimitDuringMaintenance,
            int indexBufferedWriteKeyLimit,
            int segmentCount, int segmentReadyCount,
            int segmentMaintenanceCount, int segmentErrorCount,
            int segmentClosedCount, int unloadedMappedSegmentCount,
            long totalSegmentKeys,
            long totalSegmentCacheKeys, long totalBufferedWriteKeys,
            long totalDeltaCacheFiles, long readLatencyP50Micros,
            long readLatencyP95Micros, long readLatencyP99Micros,
            long writeLatencyP50Micros, long writeLatencyP95Micros,
            long writeLatencyP99Micros, int bloomFilterHashFunctions,
            int bloomFilterIndexSizeInBytes,
            double bloomFilterProbabilityOfFalsePositive,
            long bloomFilterRequestCount, long bloomFilterRefusedCount,
            long bloomFilterPositiveCount, long bloomFilterFalsePositiveCount,
            long flushRequestCount, long compactRequestCount,
            long splitScheduleCount,
            int splitInFlightCount,
            int maintenanceQueueSize, int maintenanceQueueCapacity,
            int splitQueueSize, int splitQueueCapacity,
            double currentThroughputValue, String currentThroughputUnit,
            double compactRateValue, String compactRateUnit,
            double flushRateValue, String flushRateUnit, double splitRateValue,
            String splitRateUnit, String capturedAt,
            List<SegmentRow> segmentRuntimeSnapshots) {

        /**
         * Total operations count.
         *
         * @return sum of read/put/delete
         */
        public long totalOps() {
            return readOps + putOps + deleteOps;
        }

        /**
         * Estimated max cache keys across all segments.
         *
         * @return estimated key limit
         */
        public long estimatedSegmentCacheKeyLimit() {
            return Math.max(0L, (long) segmentCacheKeyLimitPerSegment
                    * Math.max(0, segmentCount));
        }

        /**
         * Segment cache pressure percentage based on estimated key limit.
         *
         * @return percentage in range [0..100]
         */
        public long segmentCachePressurePercent() {
            final long limit = estimatedSegmentCacheKeyLimit();
            if (limit <= 0L) {
                return 0L;
            }
            return Math.round(
                    Math.max(0L, totalSegmentCacheKeys) * 100D / limit);
        }

        /**
         * Average delta cache files per segment.
         *
         * @return average delta files/segment as compact decimal
         */
        public String averageDeltaCacheFilesPerSegmentDisplay() {
            if (segmentCount <= 0) {
                return "0";
            }
            final double average = ((double) totalDeltaCacheFiles)
                    / ((double) segmentReadyCount);
            return format4Significant(average);
        }

        /**
         * Registry cache hit ratio as percentage.
         *
         * @return percentage in range [0..100]
         */
        public long cacheHitRatioPercent() {
            final long total = cacheHitCount + cacheMissCount;
            if (total <= 0L) {
                return 0L;
            }
            return Math.round(cacheHitCount * 100D / total);
        }

        /**
         * Registry cache fill percentage.
         *
         * @return percentage in range [0..100]
         */
        public long registryCacheFillPercent() {
            if (cacheLimit <= 0) {
                return 0L;
            }
            return Math.round(Math.max(0, cacheSize) * 100D / cacheLimit);
        }

        /**
         * Throughput display with compact precision.
         *
         * @return throughput value with unit, e.g. "12.34 ops/s"
         */
        public String currentThroughputDisplay() {
            return format4Significant(currentThroughputValue) + " "
                    + currentThroughputUnit;
        }

        /**
         * Current compact requests rate display.
         *
         * @return compact rate with auto-selected unit
         */
        public String compactRateDisplay() {
            return format4Significant(compactRateValue) + " " + compactRateUnit;
        }

        /**
         * Current flush requests rate display.
         *
         * @return flush rate with auto-selected unit
         */
        public String flushRateDisplay() {
            return format4Significant(flushRateValue) + " " + flushRateUnit;
        }

        /**
         * Current split schedules rate display.
         *
         * @return split rate with auto-selected unit
         */
        public String splitRateDisplay() {
            return format4Significant(splitRateValue) + " " + splitRateUnit;
        }

        /**
         * Measured Bloom false-positive probability from runtime counters.
         *
         * This is computed as falsePositives / (falsePositives +
         * refusedByBloom), where refusedByBloom corresponds to Bloom negatives.
         *
         * @return measured false-positive probability in range [0..1]
         */
        public double bloomMeasuredFalsePositiveProbability() {
            final long falsePositives = Math.max(0L,
                    bloomFilterFalsePositiveCount);
            final long negatives = Math.max(0L, bloomFilterRefusedCount);
            final long checkedAbsent = falsePositives + negatives;
            if (checkedAbsent <= 0L) {
                return 0D;
            }
            return ((double) falsePositives) / ((double) checkedAbsent);
        }

        /**
         * Display for measured Bloom false-positive probability.
         *
         * @return value in percent form with two decimals, e.g. "1.23%"
         */
        public String bloomMeasuredFalsePositiveProbabilityDisplay() {
            final double probability = bloomMeasuredFalsePositiveProbability();
            return formatDecimal(probability * 100D, 2, 2) + "%";
        }

    }

    /**
     * Per-segment row model shown in node detail index sections.
     */
    public record SegmentRow(String segmentId, String state,
            long numberOfKeysInDeltaCache, long numberOfKeysInSegment,
            long numberOfKeysInScarceIndex, long numberOfKeysInSegmentCache,
            int numberOfKeysInWriteCache, int numberOfDeltaCacheFiles,
            long compactRequestCount, long flushRequestCount,
            long bloomFilterRequestCount, long bloomFilterRefusedCount,
            long bloomFilterPositiveCount, long bloomFilterFalsePositiveCount) {
    }

    /**
     * Action row model.
     */
    public record ActionRow(String actionId, String nodeId, String action,
            String status, String message, String updatedAt) {
    }

    /**
     * Event row model.
     */
    public record EventRow(String at, String type, String nodeId,
            String detail) {
    }

    /**
     * Runtime configuration snapshot shown in node detail editor.
     */
    public record RuntimeConfigView(String indexName,
            Map<String, Integer> original, Map<String, Integer> current,
            List<String> supportedKeys, long revision, String capturedAt) {
    }
}
