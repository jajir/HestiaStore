package org.hestiastore.console.web;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * HTTP client adapter from web UI to monitoring-console backend.
 */
@Service
public class ConsoleBackendClient {

    private static final String CONTENT_TYPE = "Content-Type";
    private static final String APPLICATION_JSON = "application/json";
    private static final String WRITE_TOKEN_HEADER = "X-Hestia-Console-Token";

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final MonitoringConsoleWebProperties properties;
    private final HttpClient httpClient;
    private final ObjectMapper mapper;
    private final ConcurrentMap<String, ThroughputSample> throughputSamples = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ThroughputSample> counterRateSamples = new ConcurrentHashMap<>();

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
    }

    /**
     * Fetches dashboard data.
     *
     * @return node rows
     */
    public List<NodeRow> fetchDashboard() {
        try {
            final JsonNode array = getJson("/console/v1/dashboard");
            final List<NodeRow> rows = new ArrayList<>();
            final long nowNanos = System.nanoTime();
            for (final JsonNode n : array) {
                final long getOps = n.path("getOperationCount").asLong(0L);
                final long putOps = n.path("putOperationCount").asLong(0L);
                final long deleteOps = n.path("deleteOperationCount").asLong(0L);
                final long compactRequestCount = n.path("compactRequestCount")
                        .asLong(0L);
                final long flushRequestCount = n.path("flushRequestCount")
                        .asLong(0L);
                final long splitScheduleCount = n.path("splitScheduleCount")
                        .asLong(0L);
                final long totalOps = Math.max(0L, getOps) + Math.max(0L, putOps)
                        + Math.max(0L, deleteOps);
                final Throughput throughput = computeThroughput(n.path("nodeId").asText(""),
                        totalOps, nowNanos);
                final String nodeId = n.path("nodeId").asText("");
                final CounterRate compactRate = computeCounterRate(
                        nodeId + ":compact", compactRequestCount, nowNanos);
                final CounterRate flushRate = computeCounterRate(
                        nodeId + ":flush", flushRequestCount, nowNanos);
                final CounterRate splitRate = computeCounterRate(
                        nodeId + ":split", splitScheduleCount, nowNanos);
                rows.add(new NodeRow(n.path("nodeId").asText(""),
                        n.path("nodeName").asText(""),
                        n.path("state").asText(""),
                        n.path("reachable").asBoolean(false),
                        n.path("ready").asBoolean(false),
                        n.path("baseUrl").asText(""),
                        getOps,
                        putOps,
                        deleteOps,
                        n.path("registryCacheHitCount").asLong(0L),
                        n.path("registryCacheMissCount").asLong(0L),
                        n.path("registryCacheLoadCount").asLong(0L),
                        n.path("registryCacheEvictionCount").asLong(0L),
                        n.path("registryCacheSize").asInt(0),
                        n.path("registryCacheLimit").asInt(0),
                        n.path("segmentCacheKeyLimitPerSegment").asInt(0),
                        n.path("maxNumberOfKeysInSegmentWriteCache").asInt(0),
                        n.path(
                                "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance")
                                .asInt(0),
                        n.path("segmentCount").asInt(0),
                        n.path("segmentReadyCount").asInt(0),
                        n.path("segmentMaintenanceCount").asInt(0),
                        n.path("segmentErrorCount").asInt(0),
                        n.path("segmentClosedCount").asInt(0),
                        n.path("segmentBusyCount").asInt(0),
                        n.path("totalSegmentKeys").asLong(0L),
                        n.path("totalSegmentCacheKeys").asLong(0L),
                        n.path("totalWriteCacheKeys").asLong(0L),
                        n.path("totalDeltaCacheFiles").asLong(0L),
                        compactRequestCount,
                        flushRequestCount,
                        splitScheduleCount,
                        compactRate.value(), compactRate.unit(),
                        flushRate.value(), flushRate.unit(),
                        splitRate.value(), splitRate.unit(),
                        n.path("splitInFlightCount").asInt(0),
                        n.path("maintenanceQueueSize").asInt(0),
                        n.path("maintenanceQueueCapacity").asInt(0),
                        n.path("splitQueueSize").asInt(0),
                        n.path("splitQueueCapacity").asInt(0),
                        n.path("readLatencyP50Micros").asLong(0L),
                        n.path("readLatencyP95Micros").asLong(0L),
                        n.path("readLatencyP99Micros").asLong(0L),
                        n.path("writeLatencyP50Micros").asLong(0L),
                        n.path("writeLatencyP95Micros").asLong(0L),
                        n.path("writeLatencyP99Micros").asLong(0L),
                        n.path("bloomFilterHashFunctions").asInt(0),
                        n.path("bloomFilterIndexSizeInBytes").asInt(0),
                        n.path("bloomFilterProbabilityOfFalsePositive")
                                .asDouble(0D),
                        n.path("bloomFilterRequestCount").asLong(0L),
                        n.path("bloomFilterRefusedCount").asLong(0L),
                        n.path("bloomFilterPositiveCount").asLong(0L),
                        n.path("bloomFilterFalsePositiveCount").asLong(0L),
                        n.path("jvmHeapUsedBytes").asLong(0L),
                        n.path("jvmHeapCommittedBytes").asLong(0L),
                        n.path("jvmNonHeapUsedBytes").asLong(0L),
                        n.path("jvmGcCount").asLong(0L),
                        n.path("jvmGcTimeMillis").asLong(0L),
                        throughput.value(), throughput.unit(),
                        n.path("pollLatencyMillis").asLong(0L),
                        asInstantText(n.path("capturedAt")),
                        n.path("error").asText("")));
            }
            return rows;
        } catch (final Exception e) {
            logger.warn("Dashboard fetch failed", e);
            return List.of();
        }
    }

    /**
     * Fetches action history.
     *
     * @return actions
     */
    public List<ActionRow> fetchActions() {
        try {
            final JsonNode array = getJson("/console/v1/actions");
            final List<ActionRow> rows = new ArrayList<>();
            for (final JsonNode n : array) {
                rows.add(new ActionRow(n.path("actionId").asText(""),
                        n.path("nodeId").asText(""),
                        n.path("action").asText(""),
                        n.path("status").asText(""),
                        n.path("message").asText(""),
                        asInstantText(n.path("updatedAt"))));
            }
            return rows;
        } catch (final Exception e) {
            logger.warn("Action fetch failed", e);
            return List.of();
        }
    }

    /**
     * Fetches event timeline.
     *
     * @return events
     */
    public List<EventRow> fetchEvents() {
        try {
            final JsonNode array = getJson("/console/v1/events");
            final List<EventRow> rows = new ArrayList<>();
            for (final JsonNode n : array) {
                rows.add(new EventRow(asInstantText(n.path("at")),
                        n.path("type").asText(""),
                        n.path("nodeId").asText(""),
                        n.path("detail").asText("")));
            }
            return rows;
        } catch (final Exception e) {
            logger.warn("Event fetch failed", e);
            return List.of();
        }
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
        postJson("/console/v1/nodes",
                Map.of("nodeId", nodeId, "nodeName", nodeName, "baseUrl",
                        baseUrl, "agentToken",
                        agentToken == null ? "" : agentToken));
    }

    /**
     * Triggers flush/compact operation.
     *
     * @param actionType flush or compact
     * @param nodeId     target node id
     */
    public void triggerAction(final String actionType, final String nodeId) {
        postJson("/console/v1/actions/" + actionType,
                Map.of("nodeId", nodeId, "requestId",
                        "web-" + Instant.now().toEpochMilli(), "confirmed",
                        true));
    }

    private JsonNode getJson(final String path)
            throws IOException, InterruptedException {
        final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(properties.backendBaseUrl() + path))
                .timeout(Duration.ofSeconds(4))
                .GET()
                .build();
        final HttpResponse<String> response = httpClient.send(request,
                HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 200 && response.statusCode() < 300) {
            return mapper.readTree(response.body());
        }
        throw new IllegalStateException(
                "GET " + path + " failed: " + response.statusCode());
    }

    private JsonNode postJson(final String path, final Map<String, Object> body) {
        try {
            final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(properties.backendBaseUrl() + path))
                    .timeout(Duration.ofSeconds(5))
                    .header(CONTENT_TYPE, APPLICATION_JSON);
            if (!properties.writeToken().isEmpty()) {
                requestBuilder.header(WRITE_TOKEN_HEADER,
                        properties.writeToken());
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

    /**
     * Dashboard row model.
     */
    public record NodeRow(String nodeId, String nodeName, String state,
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
            long jvmNonHeapUsedBytes, long jvmGcCount, long jvmGcTimeMillis,
            double currentThroughputValue, String currentThroughputUnit,
            long latencyMs, String capturedAt, String error) {
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
         * Heap used in compact human-readable units.
         *
         * @return heap usage, e.g. "8.23 GB" or "737 MB"
         */
        public String heapUsedHumanReadable() {
            return formatBytes(jvmHeapUsedBytes);
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
            final String formatted;
            if (value >= 100D) {
                formatted = String.format(java.util.Locale.US, "%.0f", value);
            } else if (value >= 10D) {
                formatted = String.format(java.util.Locale.US, "%.1f", value);
            } else {
                formatted = String.format(java.util.Locale.US, "%.2f", value);
            }
            final String compact = formatted.contains(".")
                    ? formatted.replaceAll("\\.?0+$", "")
                    : formatted;
            return compact + " " + SIZE_UNITS[unitIndex];
        }

        private static String format4Significant(final double value) {
            if (value <= 0D || Double.isNaN(value) || Double.isInfinite(value)) {
                return "0";
            }
            final BigDecimal rounded = new BigDecimal(value,
                    new MathContext(4, RoundingMode.HALF_UP))
                            .stripTrailingZeros();
            return rounded.toPlainString();
        }
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
