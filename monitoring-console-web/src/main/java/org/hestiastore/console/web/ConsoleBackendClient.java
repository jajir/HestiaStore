package org.hestiastore.console.web;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.hestiastore.index.monitoring.MonitoredIndex;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * HTTP client adapter from web UI directly to management-agent nodes.
 */
@Service
public class ConsoleBackendClient {

    private static final String CONTENT_TYPE = "Content-Type";
    private static final String APPLICATION_JSON = "application/json";
    private static final String AUTHORIZATION = "Authorization";
    private static final String API_REPORT = "/api/v1/report";
    private static final String API_ACTION_FLUSH = "/api/v1/actions/flush";
    private static final String API_ACTION_COMPACT = "/api/v1/actions/compact";
    private static final int MAX_HISTORY = 200;

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
    public ConsoleBackendClient(final MonitoringConsoleWebProperties properties) {
        this.properties = properties;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
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
            throw new IllegalArgumentException("Unsupported action: " + actionType);
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

    private NodeDetails fetchNodeDetails(
            final MonitoringConsoleWebProperties.NodeEndpoint node,
            final long nowNanos) {
        final long startedNanos = System.nanoTime();
        try {
            final JsonNode report = getJson(node, API_REPORT);
            final JsonNode jvm = report.path("jvm");
            final List<RemoteMonitoredIndex> monitoredIndexes = parseMonitoredIndexes(
                    report.path("indexes"));
            final List<IndexRow> indexRows = toIndexRows(node.nodeId(),
                    monitoredIndexes, asInstantText(report.path("capturedAt")),
                    nowNanos);
            final String resolvedIndexName = monitoredIndexes.isEmpty()
                    ? node.nodeId()
                    : monitoredIndexes.stream().map(MonitoredIndex::indexName)
                            .sorted().collect(Collectors.joining(","));
            final String resolvedNodeName = node.nodeName().isBlank()
                    ? resolvedIndexName
                    : node.nodeName();
            final long getOps = monitoredIndexes.stream()
                    .mapToLong(i -> i.metricsSnapshot().getGetOperationCount())
                    .sum();
            final long putOps = monitoredIndexes.stream()
                    .mapToLong(i -> i.metricsSnapshot().getPutOperationCount())
                    .sum();
            final long deleteOps = monitoredIndexes.stream().mapToLong(
                    i -> i.metricsSnapshot().getDeleteOperationCount()).sum();
            final long compactRequestCount = monitoredIndexes.stream()
                    .mapToLong(i -> i.metricsSnapshot().getCompactRequestCount())
                    .sum();
            final long flushRequestCount = monitoredIndexes.stream()
                    .mapToLong(i -> i.metricsSnapshot().getFlushRequestCount())
                    .sum();
            final long splitScheduleCount = monitoredIndexes.stream().mapToLong(
                    i -> i.metricsSnapshot().getSplitScheduleCount()).sum();
            final long totalOps = Math.max(0L, getOps) + Math.max(0L, putOps)
                    + Math.max(0L, deleteOps);
            final Throughput throughput = computeThroughput(node.nodeId(),
                    totalOps, nowNanos);
            final CounterRate compactRate = computeCounterRate(
                    node.nodeId() + ":compact", compactRequestCount,
                    nowNanos);
            final CounterRate flushRate = computeCounterRate(
                    node.nodeId() + ":flush", flushRequestCount, nowNanos);
            final CounterRate splitRate = computeCounterRate(
                    node.nodeId() + ":split", splitScheduleCount, nowNanos);
            final NodeRow nodeRow = new NodeRow(node.nodeId(),
                    resolvedNodeName,
                    resolvedIndexName,
                    summarizeNodeState(monitoredIndexes),
                    true,
                    !monitoredIndexes.isEmpty()
                            && monitoredIndexes.stream()
                                    .allMatch(MonitoredIndex::ready),
                    node.baseUrl(),
                    getOps,
                    putOps,
                    deleteOps,
                    monitoredIndexes.stream().mapToLong(
                            i -> i.metricsSnapshot().getRegistryCacheHitCount())
                            .sum(),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.metricsSnapshot().getRegistryCacheMissCount())
                            .sum(),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.metricsSnapshot().getRegistryCacheLoadCount())
                            .sum(),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.metricsSnapshot()
                                    .getRegistryCacheEvictionCount())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.metricsSnapshot().getRegistryCacheSize())
                            .sum(),
                    monitoredIndexes.stream().mapToInt(
                            i -> i.metricsSnapshot().getRegistryCacheLimit())
                            .sum(),
                    monitoredIndexes.stream().mapToInt(i -> i.metricsSnapshot()
                            .getSegmentCacheKeyLimitPerSegment()).sum(),
                    monitoredIndexes.stream().mapToInt(i -> i.metricsSnapshot()
                            .getMaxNumberOfKeysInSegmentWriteCache()).sum(),
                    monitoredIndexes.stream().mapToInt(i -> i.metricsSnapshot()
                            .getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance())
                            .sum(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.metricsSnapshot().getSegmentCount())
                            .sum(),
                    monitoredIndexes.stream().mapToInt(
                            i -> i.metricsSnapshot().getSegmentReadyCount())
                            .sum(),
                    monitoredIndexes.stream().mapToInt(i -> i.metricsSnapshot()
                            .getSegmentMaintenanceCount()).sum(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.metricsSnapshot().getSegmentErrorCount())
                            .sum(),
                    monitoredIndexes.stream().mapToInt(
                            i -> i.metricsSnapshot().getSegmentClosedCount())
                            .sum(),
                    monitoredIndexes.stream().mapToInt(
                            i -> i.metricsSnapshot().getSegmentBusyCount()).sum(),
                    monitoredIndexes.stream()
                            .mapToLong(i -> i.metricsSnapshot().getTotalSegmentKeys())
                            .sum(),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.metricsSnapshot().getTotalSegmentCacheKeys())
                            .sum(),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.metricsSnapshot().getTotalWriteCacheKeys())
                            .sum(),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.metricsSnapshot().getTotalDeltaCacheFiles())
                            .sum(),
                    compactRequestCount,
                    flushRequestCount,
                    splitScheduleCount,
                    compactRate.value(), compactRate.unit(),
                    flushRate.value(), flushRate.unit(),
                    splitRate.value(), splitRate.unit(),
                    monitoredIndexes.stream().mapToInt(
                            i -> i.metricsSnapshot().getSplitInFlightCount())
                            .sum(),
                    monitoredIndexes.stream().mapToInt(i -> i.metricsSnapshot()
                            .getMaintenanceQueueSize()).sum(),
                    monitoredIndexes.stream().mapToInt(i -> i.metricsSnapshot()
                            .getMaintenanceQueueCapacity()).sum(),
                    monitoredIndexes.stream()
                            .mapToInt(i -> i.metricsSnapshot().getSplitQueueSize())
                            .sum(),
                    monitoredIndexes.stream().mapToInt(
                            i -> i.metricsSnapshot().getSplitQueueCapacity())
                            .sum(),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.metricsSnapshot().getReadLatencyP50Micros())
                            .max().orElse(0L),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.metricsSnapshot().getReadLatencyP95Micros())
                            .max().orElse(0L),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.metricsSnapshot().getReadLatencyP99Micros())
                            .max().orElse(0L),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.metricsSnapshot().getWriteLatencyP50Micros())
                            .max().orElse(0L),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.metricsSnapshot().getWriteLatencyP95Micros())
                            .max().orElse(0L),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.metricsSnapshot().getWriteLatencyP99Micros())
                            .max().orElse(0L),
                    monitoredIndexes.stream().mapToInt(i -> i.metricsSnapshot()
                            .getBloomFilterHashFunctions()).max().orElse(0),
                    monitoredIndexes.stream().mapToInt(i -> i.metricsSnapshot()
                            .getBloomFilterIndexSizeInBytes()).sum(),
                    monitoredIndexes.stream()
                            .mapToDouble(i -> i.metricsSnapshot()
                                    .getBloomFilterProbabilityOfFalsePositive())
                            .max().orElse(0D),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.metricsSnapshot().getBloomFilterRequestCount())
                            .sum(),
                    monitoredIndexes.stream().mapToLong(
                            i -> i.metricsSnapshot().getBloomFilterRefusedCount())
                            .sum(),
                    monitoredIndexes.stream().mapToLong(i -> i.metricsSnapshot()
                            .getBloomFilterPositiveCount()).sum(),
                    monitoredIndexes.stream().mapToLong(i -> i.metricsSnapshot()
                            .getBloomFilterFalsePositiveCount()).sum(),
                    Math.max(0L, jvm.path("heapUsedBytes").asLong(0L)),
                    Math.max(0L, jvm.path("heapCommittedBytes").asLong(0L)),
                    Math.max(0L, jvm.path("heapMaxBytes").asLong(0L)),
                    Math.max(0L, jvm.path("nonHeapUsedBytes").asLong(0L)),
                    Math.max(0L, jvm.path("gcCount").asLong(0L)),
                    Math.max(0L, jvm.path("gcTimeMillis").asLong(0L)),
                    throughput.value(), throughput.unit(),
                    Duration.ofNanos(System.nanoTime() - startedNanos)
                            .toMillis(),
                    asInstantText(report.path("capturedAt")), "");
            return new NodeDetails(nodeRow, indexRows);
        } catch (final Exception e) {
            logger.warn("Node poll failed: {}", node.nodeId(), e);
            addEvent(new EventRow(Instant.now().toString(), "NODE_POLL_FAILED",
                    node.nodeId(), e.getMessage() == null ? "poll failed"
                            : e.getMessage()));
            return new NodeDetails(
                    unavailable(node, e.getMessage(), startedNanos), List.of());
        }
    }

    private NodeRow unavailable(
            final MonitoringConsoleWebProperties.NodeEndpoint node,
            final String error, final long startedNanos) {
        return new NodeRow(node.nodeId(), node.nodeName(), "",
                "UNAVAILABLE", false,
                false, node.baseUrl(), 0L, 0L, 0L,
                0L, 0L, 0L, 0L,
                0, 0, 0,
                0, 0,
                0, 0, 0, 0, 0,
                0,
                0L, 0L, 0L, 0L,
                0L, 0L, 0L,
                0D, "/s", 0D, "/s", 0D, "/s",
                0,
                0, 0, 0, 0,
                0L, 0L, 0L, 0L, 0L, 0L,
                0, 0, 0D,
                0L, 0L, 0L, 0L,
                0L, 0L, 0L, 0L, 0L, 0L,
                0D, "ops/s",
                Duration.ofNanos(System.nanoTime() - startedNanos).toMillis(),
                Instant.now().toString(), error == null ? "" : error);
    }

    private JsonNode getJson(final MonitoringConsoleWebProperties.NodeEndpoint node,
            final String path) throws IOException, InterruptedException {
        final HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(node.baseUrl() + path))
                .timeout(Duration.ofSeconds(4))
                .GET();
        if (!node.agentToken().isEmpty()) {
            builder.header(AUTHORIZATION, "Bearer " + node.agentToken());
        }
        final HttpResponse<String> response = httpClient.send(builder.build(),
                HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 200 && response.statusCode() < 300) {
            return mapper.readTree(response.body());
        }
        throw new IllegalStateException(
                "GET " + path + " failed: " + response.statusCode());
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
                        "Bearer " + node.agentToken());
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
            throw new IllegalStateException("POST " + path + " failed: "
                    + response.statusCode() + " body=" + response.body());
        } catch (final Exception e) {
            throw new IllegalStateException("POST " + path + " failed", e);
        }
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

    private List<IndexRow> toIndexRows(final String nodeId,
            final List<RemoteMonitoredIndex> monitoredIndexes,
            final String capturedAt, final long nowNanos) {
        final List<IndexRow> rows = new ArrayList<>();
        for (final RemoteMonitoredIndex monitoredIndex : monitoredIndexes) {
            final SegmentIndexMetricsSnapshot snapshot = monitoredIndex
                    .metricsSnapshot();
            final long totalOps = nonNegativeLong(snapshot.getGetOperationCount())
                    + nonNegativeLong(snapshot.getPutOperationCount())
                    + nonNegativeLong(snapshot.getDeleteOperationCount());
            final Throughput throughput = computeThroughput(
                    nodeId + ":" + monitoredIndex.indexName(), totalOps,
                    nowNanos);
            final CounterRate compactRate = computeCounterRate(
                    nodeId + ":" + monitoredIndex.indexName() + ":compact",
                    snapshot.getCompactRequestCount(), nowNanos);
            final CounterRate flushRate = computeCounterRate(
                    nodeId + ":" + monitoredIndex.indexName() + ":flush",
                    snapshot.getFlushRequestCount(), nowNanos);
            final CounterRate splitRate = computeCounterRate(
                    nodeId + ":" + monitoredIndex.indexName() + ":split",
                    snapshot.getSplitScheduleCount(), nowNanos);
            rows.add(new IndexRow(
                    monitoredIndex.indexName(),
                    monitoredIndex.state().name(),
                    monitoredIndex.ready(),
                    snapshot.getGetOperationCount(),
                    snapshot.getPutOperationCount(),
                    snapshot.getDeleteOperationCount(),
                    snapshot.getRegistryCacheHitCount(),
                    snapshot.getRegistryCacheMissCount(),
                    snapshot.getRegistryCacheLoadCount(),
                    snapshot.getRegistryCacheEvictionCount(),
                    snapshot.getRegistryCacheSize(),
                    snapshot.getRegistryCacheLimit(),
                    snapshot.getSegmentCacheKeyLimitPerSegment(),
                    snapshot.getMaxNumberOfKeysInSegmentWriteCache(),
                    snapshot.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(),
                    snapshot.getSegmentCount(),
                    snapshot.getSegmentReadyCount(),
                    snapshot.getSegmentMaintenanceCount(),
                    snapshot.getSegmentErrorCount(),
                    snapshot.getSegmentClosedCount(),
                    snapshot.getSegmentBusyCount(),
                    snapshot.getTotalSegmentKeys(),
                    snapshot.getTotalSegmentCacheKeys(),
                    snapshot.getTotalWriteCacheKeys(),
                    snapshot.getTotalDeltaCacheFiles(),
                    snapshot.getReadLatencyP50Micros(),
                    snapshot.getReadLatencyP95Micros(),
                    snapshot.getReadLatencyP99Micros(),
                    snapshot.getWriteLatencyP50Micros(),
                    snapshot.getWriteLatencyP95Micros(),
                    snapshot.getWriteLatencyP99Micros(),
                    snapshot.getBloomFilterHashFunctions(),
                    snapshot.getBloomFilterIndexSizeInBytes(),
                    snapshot.getBloomFilterProbabilityOfFalsePositive(),
                    snapshot.getBloomFilterRequestCount(),
                    snapshot.getBloomFilterRefusedCount(),
                    snapshot.getBloomFilterPositiveCount(),
                    snapshot.getBloomFilterFalsePositiveCount(),
                    snapshot.getFlushRequestCount(),
                    snapshot.getCompactRequestCount(),
                    snapshot.getSplitScheduleCount(),
                    snapshot.getSplitInFlightCount(),
                    snapshot.getMaintenanceQueueSize(),
                    snapshot.getMaintenanceQueueCapacity(),
                    snapshot.getSplitQueueSize(),
                    snapshot.getSplitQueueCapacity(),
                    throughput.value(),
                    throughput.unit(),
                    compactRate.value(),
                    compactRate.unit(),
                    flushRate.value(),
                    flushRate.unit(),
                    splitRate.value(),
                    splitRate.unit(),
                    capturedAt));
        }
        rows.sort(Comparator.comparing(IndexRow::indexName));
        return List.copyOf(rows);
    }

    private List<RemoteMonitoredIndex> parseMonitoredIndexes(
            final JsonNode indexesNode) {
        if (indexesNode == null || !indexesNode.isArray()) {
            return List.of();
        }
        final List<RemoteMonitoredIndex> parsed = new ArrayList<>();
        for (final JsonNode indexNode : indexesNode) {
            parsed.add(new RemoteMonitoredIndex(
                    indexNode.path("indexName").asText("unknown-index"),
                    parseState(indexNode.path("state").asText("ERROR")),
                    new SegmentIndexMetricsSnapshot(
                            nonNegativeLong(
                                    indexNode.path("getOperationCount").asLong(0L)),
                            nonNegativeLong(
                                    indexNode.path("putOperationCount").asLong(0L)),
                            nonNegativeLong(indexNode.path("deleteOperationCount")
                                    .asLong(0L)),
                            nonNegativeLong(indexNode.path("registryCacheHitCount")
                                    .asLong(0L)),
                            nonNegativeLong(indexNode.path("registryCacheMissCount")
                                    .asLong(0L)),
                            nonNegativeLong(indexNode.path("registryCacheLoadCount")
                                    .asLong(0L)),
                            nonNegativeLong(indexNode.path("registryCacheEvictionCount")
                                    .asLong(0L)),
                            nonNegativeInt(indexNode.path("registryCacheSize").asInt(0)),
                            nonNegativeInt(indexNode.path("registryCacheLimit").asInt(0)),
                            nonNegativeInt(indexNode.path("segmentCacheKeyLimitPerSegment")
                                    .asInt(0)),
                            nonNegativeInt(indexNode.path("maxNumberOfKeysInSegmentWriteCache")
                                    .asInt(0)),
                            nonNegativeInt(indexNode
                                    .path(
                                            "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance")
                                    .asInt(0)),
                            nonNegativeInt(indexNode.path("segmentCount").asInt(0)),
                            nonNegativeInt(
                                    indexNode.path("segmentReadyCount").asInt(0)),
                            nonNegativeInt(indexNode.path("segmentMaintenanceCount")
                                    .asInt(0)),
                            nonNegativeInt(indexNode.path("segmentErrorCount").asInt(0)),
                            nonNegativeInt(indexNode.path("segmentClosedCount").asInt(0)),
                            nonNegativeInt(indexNode.path("segmentBusyCount").asInt(0)),
                            nonNegativeLong(indexNode.path("totalSegmentKeys").asLong(0L)),
                            nonNegativeLong(
                                    indexNode.path("totalSegmentCacheKeys").asLong(0L)),
                            nonNegativeLong(
                                    indexNode.path("totalWriteCacheKeys").asLong(0L)),
                            nonNegativeLong(
                                    indexNode.path("totalDeltaCacheFiles").asLong(0L)),
                            nonNegativeLong(indexNode.path("compactRequestCount").asLong(0L)),
                            nonNegativeLong(indexNode.path("flushRequestCount").asLong(0L)),
                            nonNegativeLong(indexNode.path("splitScheduleCount").asLong(0L)),
                            nonNegativeInt(indexNode.path("splitInFlightCount").asInt(0)),
                            nonNegativeInt(indexNode.path("maintenanceQueueSize").asInt(0)),
                            nonNegativeInt(
                                    indexNode.path("maintenanceQueueCapacity").asInt(0)),
                            nonNegativeInt(indexNode.path("splitQueueSize").asInt(0)),
                            nonNegativeInt(indexNode.path("splitQueueCapacity").asInt(0)),
                            nonNegativeLong(indexNode.path("readLatencyP50Micros").asLong(0L)),
                            nonNegativeLong(indexNode.path("readLatencyP95Micros").asLong(0L)),
                            nonNegativeLong(indexNode.path("readLatencyP99Micros").asLong(0L)),
                            nonNegativeLong(
                                    indexNode.path("writeLatencyP50Micros").asLong(0L)),
                            nonNegativeLong(
                                    indexNode.path("writeLatencyP95Micros").asLong(0L)),
                            nonNegativeLong(
                                    indexNode.path("writeLatencyP99Micros").asLong(0L)),
                            nonNegativeInt(indexNode.path("bloomFilterHashFunctions").asInt(0)),
                            nonNegativeInt(
                                    indexNode.path("bloomFilterIndexSizeInBytes").asInt(0)),
                            Math.max(0D, indexNode
                                    .path("bloomFilterProbabilityOfFalsePositive")
                                    .asDouble(0D)),
                            nonNegativeLong(
                                    indexNode.path("bloomFilterRequestCount").asLong(0L)),
                            nonNegativeLong(
                                    indexNode.path("bloomFilterRefusedCount").asLong(0L)),
                            nonNegativeLong(
                                    indexNode.path("bloomFilterPositiveCount").asLong(0L)),
                            nonNegativeLong(indexNode.path("bloomFilterFalsePositiveCount")
                                    .asLong(0L)),
                            parseState(indexNode.path("state").asText("ERROR")))));
        }
        return List.copyOf(parsed);
    }

    private SegmentIndexState parseState(final String state) {
        if (state == null) {
            return SegmentIndexState.ERROR;
        }
        try {
            return SegmentIndexState.valueOf(state.trim().toUpperCase(Locale.ROOT));
        } catch (final IllegalArgumentException e) {
            return SegmentIndexState.ERROR;
        }
    }

    private String summarizeNodeState(final List<RemoteMonitoredIndex> indexes) {
        if (indexes.isEmpty()) {
            return "UNAVAILABLE";
        }
        if (indexes.stream().allMatch(MonitoredIndex::ready)) {
            return SegmentIndexState.READY.name();
        }
        if (indexes.stream().anyMatch(i -> i.state() == SegmentIndexState.ERROR)) {
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

    /**
     * Dashboard row model.
     */
    public record NodeRow(String nodeId, String nodeName, String indexName,
            String state,
            boolean reachable, boolean ready, String baseUrl, long getOps,
            long putOps, long deleteOps, long cacheHitCount,
            long cacheMissCount, long cacheLoadCount, long cacheEvictionCount,
            int cacheSize, int cacheLimit, int segmentCacheKeyLimitPerSegment,
            int maxNumberOfKeysInSegmentWriteCache,
            int maxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
            int segmentCount,
            int segmentReadyCount, int segmentMaintenanceCount,
            int segmentErrorCount, int segmentClosedCount, int segmentBusyCount,
            long totalSegmentKeys, long totalSegmentCacheKeys,
            long totalWriteCacheKeys, long totalDeltaCacheFiles,
            long compactRequestCount, long flushRequestCount,
            long splitScheduleCount, double compactRateValue,
            String compactRateUnit, double flushRateValue,
            String flushRateUnit, double splitRateValue, String splitRateUnit,
            int splitInFlightCount,
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
            long jvmGcTimeMillis,
            double currentThroughputValue, String currentThroughputUnit,
            long latencyMs, String capturedAt, String error) {
        private static final Locale NUMBER_LOCALE = Locale.US;
        private static final char THOUSANDS_SEPARATOR = ',';
        private static final char DECIMAL_SEPARATOR = '.';
        private static final String NUMBER_SEPARATOR_NOTE =
                "Numeric separators: thousands '" + THOUSANDS_SEPARATOR
                        + "', decimal '" + DECIMAL_SEPARATOR + "'.";
        private static final double BYTES_PER_GIB = 1024D * 1024D * 1024D;
        private static final String[] SIZE_UNITS = { "B", "KB", "MB", "GB",
                "TB", "PB" };

        /**
         * Total operations count.
         *
         * @return sum of get/put/delete
         */
        public long totalOps() {
            return getOps + putOps + deleteOps;
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
            return String.format(NUMBER_LOCALE, "%,d", value);
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
            return Math.round((heapFreeBytes() * 100D) / jvmHeapCommittedBytes);
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
            return Math.round((Math.max(0L, totalSegmentCacheKeys) * 100D)
                    / limit);
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
            return Math.round((Math.max(0, cacheSize) * 100D) / cacheLimit);
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
            final BigDecimal rounded = BigDecimal.valueOf(value).round(
                    new MathContext(4, RoundingMode.HALF_UP))
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
            format.setMaximumFractionDigits(Math.max(minFractionDigits,
                    maxFractionDigits));
            return format.format(value);
        }

    }

    private record RemoteMonitoredIndex(String indexName, SegmentIndexState state,
            SegmentIndexMetricsSnapshot metricsSnapshot)
            implements MonitoredIndex {
    }

    private Throughput computeThroughput(final String nodeId, final long totalOps,
            final long nowNanos) {
        if (nodeId == null || nodeId.isBlank()) {
            return new Throughput(0D, "ops/s");
        }
        final ThroughputSample previous = throughputSamples.put(nodeId,
                new ThroughputSample(totalOps, nowNanos));
        if (previous == null) {
            return new Throughput(0D, "ops/s");
        }
        final long deltaOps = totalOps - previous.totalOps();
        final long deltaNanos = nowNanos - previous.atNanos();
        if (deltaOps <= 0L || deltaNanos <= 0L) {
            return new Throughput(0D, "ops/s");
        }
        double value = (deltaOps * 1_000_000_000D) / deltaNanos;
        String unit = "ops/s";
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
        final double perSecond = (deltaCount * 1_000_000_000D) / deltaNanos;
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
            long getOps, long putOps, long deleteOps, long cacheHitCount,
            long cacheMissCount, long cacheLoadCount, long cacheEvictionCount,
            int cacheSize, int cacheLimit, int segmentCacheKeyLimitPerSegment,
            int maxNumberOfKeysInSegmentWriteCache,
            int maxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
            int segmentCount, int segmentReadyCount,
            int segmentMaintenanceCount, int segmentErrorCount,
            int segmentClosedCount, int segmentBusyCount,
            long totalSegmentKeys, long totalSegmentCacheKeys,
            long totalWriteCacheKeys, long totalDeltaCacheFiles,
            long readLatencyP50Micros, long readLatencyP95Micros,
            long readLatencyP99Micros, long writeLatencyP50Micros,
            long writeLatencyP95Micros, long writeLatencyP99Micros,
            int bloomFilterHashFunctions, int bloomFilterIndexSizeInBytes,
            double bloomFilterProbabilityOfFalsePositive,
            long bloomFilterRequestCount, long bloomFilterRefusedCount,
            long bloomFilterPositiveCount, long bloomFilterFalsePositiveCount,
            long flushRequestCount, long compactRequestCount,
            long splitScheduleCount, int splitInFlightCount,
            int maintenanceQueueSize, int maintenanceQueueCapacity,
            int splitQueueSize, int splitQueueCapacity,
            double currentThroughputValue, String currentThroughputUnit,
            double compactRateValue, String compactRateUnit,
            double flushRateValue, String flushRateUnit, double splitRateValue,
            String splitRateUnit,
            String capturedAt) {

        /**
         * Total operations count.
         *
         * @return sum of get/put/delete
         */
        public long totalOps() {
            return getOps + putOps + deleteOps;
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
            return Math.round((Math.max(0L, totalSegmentCacheKeys) * 100D)
                    / limit);
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
            return Math.round((cacheHitCount * 100D) / total);
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
            return Math.round((Math.max(0, cacheSize) * 100D) / cacheLimit);
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

        private static String format4Significant(final double value) {
            if (value <= 0D || Double.isNaN(value) || Double.isInfinite(value)) {
                return "0";
            }
            final BigDecimal rounded = BigDecimal.valueOf(value).round(
                    new MathContext(4, RoundingMode.HALF_UP))
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
                    .getInstance(Locale.US);
            final DecimalFormat format = new DecimalFormat("#,##0.############",
                    symbols);
            format.setGroupingUsed(true);
            format.setMinimumFractionDigits(Math.max(0, minFractionDigits));
            format.setMaximumFractionDigits(Math.max(minFractionDigits,
                    maxFractionDigits));
            return format.format(value);
        }
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
    public record EventRow(String at, String type, String nodeId, String detail) {
    }
}
