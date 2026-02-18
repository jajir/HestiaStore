package org.hestiastore.console;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;

import org.hestiastore.management.api.ActionRequest;
import org.hestiastore.management.api.ActionResponse;
import org.hestiastore.management.api.ActionStatus;
import org.hestiastore.management.api.ActionType;
import org.hestiastore.management.api.ErrorResponse;
import org.hestiastore.management.api.ManagementApiPaths;
import org.hestiastore.management.api.MetricsResponse;
import org.hestiastore.management.api.NodeStateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * Central monitoring console over multiple node-local management agents.
 */
public final class MonitoringConsoleServer implements AutoCloseable {

    private static final String METHOD_GET = "GET";
    private static final String METHOD_POST = "POST";
    private static final String METHOD_DELETE = "DELETE";
    private static final int MAX_EVENTS = 200;

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final HttpServer server;
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2)).build();
    private final ConcurrentMap<String, RegisteredNode> nodes = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConsoleActionStatusResponse> actions = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, LongAdder> actionRequestCounters = new ConcurrentHashMap<>();
    private final Deque<ConsoleEvent> events = new ConcurrentLinkedDeque<>();
    private final ExecutorService actionExecutor = Executors
            .newFixedThreadPool(2);
    private final String writeToken;
    private final boolean requireTlsToNodes;
    private final int actionRetryAttempts;

    /**
     * Creates a monitoring console server.
     *
     * @param bindAddress host/IP bind address
     * @param bindPort    bind port, 0 for random free port
     * @param writeToken  optional write token, empty means writes are open
     * @throws IOException when server creation fails
     */
    public MonitoringConsoleServer(final String bindAddress, final int bindPort,
            final String writeToken) throws IOException {
        this(bindAddress, bindPort, writeToken, false, 3);
    }

    /**
     * Creates a monitoring console server.
     *
     * @param bindAddress        host/IP bind address
     * @param bindPort           bind port, 0 for random free port
     * @param writeToken         optional write token, empty means writes are open
     * @param requireTlsToNodes  true to accept only https node URLs
     * @param actionRetryAttempts attempts for mutating action calls
     * @throws IOException when server creation fails
     */
    public MonitoringConsoleServer(final String bindAddress, final int bindPort,
            final String writeToken, final boolean requireTlsToNodes,
            final int actionRetryAttempts) throws IOException {
        this.writeToken = writeToken == null ? "" : writeToken.trim();
        this.requireTlsToNodes = requireTlsToNodes;
        if (actionRetryAttempts <= 0) {
            throw new IllegalArgumentException(
                    "actionRetryAttempts must be > 0");
        }
        this.actionRetryAttempts = actionRetryAttempts;
        this.server = HttpServer.create(
                new InetSocketAddress(
                        Objects.requireNonNull(bindAddress, "bindAddress"),
                        bindPort),
                0);
        registerRoutes();
    }

    /**
     * Starts the console HTTP server.
     */
    public void start() {
        server.start();
    }

    /**
     * Returns current local TCP port.
     *
     * @return local listening port
     */
    public int getPort() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
        actionExecutor.shutdownNow();
    }

    private void registerRoutes() {
        server.createContext("/console/v1/nodes", exchange -> safeHandle(
                exchange, this::handleNodes));
        server.createContext("/console/v1/dashboard",
                exchange -> safeHandle(exchange, this::handleDashboard));
        server.createContext("/console/v1/actions/flush",
                exchange -> safeHandle(exchange,
                        e -> handleActionSubmit(e, ActionType.FLUSH)));
        server.createContext("/console/v1/actions/compact",
                exchange -> safeHandle(exchange,
                        e -> handleActionSubmit(e, ActionType.COMPACT)));
        server.createContext("/console/v1/actions", exchange -> safeHandle(
                exchange, this::handleActionStatus));
        server.createContext("/console/v1/events",
                exchange -> safeHandle(exchange, this::handleEvents));
    }

    private void safeHandle(final HttpExchange exchange, final Handler handler)
            throws IOException {
        try {
            handler.handle(exchange);
        } catch (final RequestHandledException ignored) {
            // Response already written in request decoding helper.
        } catch (final Exception e) {
            logger.error("Console request failed: method={} path={}",
                    exchange.getRequestMethod(), exchange.getRequestURI(), e);
            writeError(exchange, 500, "INTERNAL_ERROR",
                    "Unexpected console failure.", "");
        }
    }

    private void handleNodes(final HttpExchange exchange) throws IOException {
        final String path = exchange.getRequestURI().getPath();
        if (METHOD_POST.equals(exchange.getRequestMethod())
                && "/console/v1/nodes".equals(path)) {
            if (!requireWriteAccess(exchange)) {
                return;
            }
            final NodeRegistrationRequest request = readRequest(exchange,
                    NodeRegistrationRequest.class);
            final RegisteredNode node = new RegisteredNode(request.nodeId(),
                    request.nodeName(), normalizeBaseUrl(request.baseUrl()),
                    normalizeOptional(request.agentToken()), Instant.now());
            nodes.put(node.nodeId(), node);
            addEvent("NODE_REGISTERED", node.nodeId(),
                    "Registered node " + node.nodeName());
            writeJson(exchange, 201, node);
            return;
        }
        if (METHOD_GET.equals(exchange.getRequestMethod())
                && "/console/v1/nodes".equals(path)) {
            final List<RegisteredNode> registered = nodes.values().stream()
                    .sorted((a, b) -> a.nodeId().compareTo(b.nodeId())).toList();
            writeJson(exchange, 200, registered);
            return;
        }
        if (METHOD_DELETE.equals(exchange.getRequestMethod())
                && path.startsWith("/console/v1/nodes/")) {
            if (!requireWriteAccess(exchange)) {
                return;
            }
            final String nodeId = path.substring("/console/v1/nodes/".length());
            final RegisteredNode removed = nodes.remove(nodeId);
            if (removed == null) {
                writeError(exchange, 404, "NODE_NOT_FOUND",
                        "Unknown nodeId '" + nodeId + "'.", "");
                return;
            }
            addEvent("NODE_REMOVED", nodeId,
                    "Removed node " + removed.nodeName());
            writeNoContent(exchange);
            return;
        }
        writeMethodNotAllowed(exchange);
    }

    private void handleDashboard(final HttpExchange exchange) throws IOException {
        if (!METHOD_GET.equals(exchange.getRequestMethod())) {
            writeMethodNotAllowed(exchange);
            return;
        }
        final List<NodeDashboardEntry> dashboard = new ArrayList<>();
        for (final RegisteredNode node : nodes.values()) {
            dashboard.add(fetchNodeSnapshot(node));
        }
        dashboard.sort((a, b) -> a.nodeId().compareTo(b.nodeId()));
        writeJson(exchange, 200, dashboard);
    }

    private void handleActionSubmit(final HttpExchange exchange,
            final ActionType actionType) throws IOException {
        if (!METHOD_POST.equals(exchange.getRequestMethod())) {
            writeMethodNotAllowed(exchange);
            return;
        }
        if (!requireWriteAccess(exchange)) {
            return;
        }
        final ConsoleActionRequest request = readRequest(exchange,
                ConsoleActionRequest.class);
        if (!request.confirmed()) {
            writeError(exchange, 400, "CONFIRMATION_REQUIRED",
                    "Action must be explicitly confirmed.", request.requestId());
            return;
        }
        final RegisteredNode node = nodes.get(request.nodeId());
        if (node == null) {
            writeError(exchange, 404, "NODE_NOT_FOUND",
                    "Unknown nodeId '" + request.nodeId() + "'.",
                    request.requestId());
            return;
        }
        final String actionId = UUID.randomUUID().toString();
        final Instant now = Instant.now();
        final ConsoleActionStatusResponse pending = new ConsoleActionStatusResponse(
                actionId, node.nodeId(), actionType.name(), "PENDING", "",
                now, now);
        actions.put(actionId, pending);
        actionRequestCounters
                .computeIfAbsent(actionCounterKey(node.nodeId(), actionType),
                        ignored -> new LongAdder())
                .increment();
        addEvent("ACTION_PENDING", node.nodeId(),
                actionType.name() + " requestId=" + request.requestId());
        CompletableFuture.runAsync(() -> executeAction(node, actionType, request,
                actionId), actionExecutor);
        writeJson(exchange, 202, pending);
    }

    private void executeAction(final RegisteredNode node,
            final ActionType actionType, final ConsoleActionRequest request,
            final String actionId) {
        try {
            final ActionResponse response = invokeNodeAction(node, actionType,
                    request.requestId());
            final String lifecycle = response.status() == ActionStatus.COMPLETED
                    ? "SUCCESS"
                    : "FAILED";
            final String message = response.message() == null ? ""
                    : response.message();
            final ConsoleActionStatusResponse updated = new ConsoleActionStatusResponse(
                    actionId, node.nodeId(), actionType.name(), lifecycle,
                    message, actions.get(actionId).createdAt(), Instant.now());
            actions.put(actionId, updated);
            addEvent("ACTION_" + lifecycle, node.nodeId(),
                    actionType.name() + " requestId=" + request.requestId());
        } catch (final Exception e) {
            final ConsoleActionStatusResponse current = actions.get(actionId);
            final ConsoleActionStatusResponse failed = new ConsoleActionStatusResponse(
                    actionId, node.nodeId(), actionType.name(), "FAILED",
                    e.getMessage() == null ? "action execution failed"
                            : e.getMessage(),
                    current.createdAt(), Instant.now());
            actions.put(actionId, failed);
            addEvent("ACTION_FAILED", node.nodeId(), actionType.name()
                    + " requestId=" + request.requestId() + " reason="
                    + failed.message());
        }
    }

    private ActionResponse invokeNodeAction(final RegisteredNode node,
            final ActionType actionType, final String requestId)
            throws IOException, InterruptedException {
        final String path = actionType == ActionType.FLUSH
                ? ManagementApiPaths.ACTION_FLUSH
                : ManagementApiPaths.ACTION_COMPACT;
        final ActionRequest payload = new ActionRequest(requestId);
        IOException ioFailure = null;
        IllegalStateException statusFailure = null;
        for (int attempt = 1; attempt <= actionRetryAttempts; attempt++) {
            final HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(node.baseUrl() + path))
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json");
            if (!node.agentToken().isEmpty()) {
                builder.header("Authorization",
                        "Bearer " + node.agentToken());
            }
            final HttpRequest request = builder
                    .POST(HttpRequest.BodyPublishers
                            .ofString(objectMapper.writeValueAsString(payload)))
                    .build();
            final HttpResponse<String> response;
            try {
                response = httpClient.send(request,
                        HttpResponse.BodyHandlers.ofString());
            } catch (final IOException e) {
                ioFailure = e;
                if (attempt < actionRetryAttempts) {
                    backoff(attempt);
                    continue;
                }
                throw e;
            }
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return objectMapper.readValue(response.body(),
                        ActionResponse.class);
            }
            statusFailure = new IllegalStateException(
                    "Agent returned status " + response.statusCode());
            if (response.statusCode() >= 500 && attempt < actionRetryAttempts) {
                backoff(attempt);
                continue;
            }
            throw statusFailure;
        }
        if (ioFailure != null) {
            throw ioFailure;
        }
        if (statusFailure != null) {
            throw statusFailure;
        }
        throw new IllegalStateException("Action invocation failed");
    }

    private void handleActionStatus(final HttpExchange exchange)
            throws IOException {
        if (!METHOD_GET.equals(exchange.getRequestMethod())) {
            writeMethodNotAllowed(exchange);
            return;
        }
        final String path = exchange.getRequestURI().getPath();
        if ("/console/v1/actions".equals(path)) {
            final List<ConsoleActionStatusResponse> all = actions.values().stream()
                    .sorted((a, b) -> b.updatedAt().compareTo(a.updatedAt()))
                    .toList();
            writeJson(exchange, 200, all);
            return;
        }
        if (!path.startsWith("/console/v1/actions/")
                || path.length() <= "/console/v1/actions/".length()) {
            writeError(exchange, 404, "ACTION_NOT_FOUND",
                    "Action id is missing.", "");
            return;
        }
        final String actionId = path.substring("/console/v1/actions/".length());
        final ConsoleActionStatusResponse status = actions.get(actionId);
        if (status == null) {
            writeError(exchange, 404, "ACTION_NOT_FOUND",
                    "Unknown actionId '" + actionId + "'.", "");
            return;
        }
        writeJson(exchange, 200, status);
    }

    private void handleEvents(final HttpExchange exchange) throws IOException {
        if (!METHOD_GET.equals(exchange.getRequestMethod())) {
            writeMethodNotAllowed(exchange);
            return;
        }
        writeJson(exchange, 200, List.copyOf(events));
    }

    private NodeDashboardEntry fetchNodeSnapshot(final RegisteredNode node) {
        final long startedNanos = System.nanoTime();
        try {
            final HttpResponse<String> stateRaw = sendGet(node.baseUrl()
                    + ManagementApiPaths.STATE, node.agentToken());
            final HttpResponse<String> metricsRaw = sendGet(node.baseUrl()
                    + ManagementApiPaths.METRICS, node.agentToken());
            if (stateRaw.statusCode() != 200 || metricsRaw.statusCode() != 200) {
                return unavailable(node, "Unexpected status state="
                        + stateRaw.statusCode() + " metrics="
                        + metricsRaw.statusCode(), startedNanos);
            }
            final NodeStateResponse state = objectMapper.readValue(
                    stateRaw.body(), NodeStateResponse.class);
            final MetricsResponse metrics = objectMapper.readValue(
                    metricsRaw.body(), MetricsResponse.class);
            final long compactRequestCount = Math.max(
                    metrics.compactRequestCount(),
                    getActionRequestCount(node.nodeId(), ActionType.COMPACT));
            final long flushRequestCount = Math.max(
                    metrics.flushRequestCount(),
                    getActionRequestCount(node.nodeId(), ActionType.FLUSH));
            return new NodeDashboardEntry(node.nodeId(), node.nodeName(),
                    node.baseUrl(), true, state.state(), state.ready(),
                    metrics.getOperationCount(), metrics.putOperationCount(),
                    metrics.deleteOperationCount(),
                    metrics.registryCacheHitCount(),
                    metrics.registryCacheMissCount(),
                    metrics.registryCacheLoadCount(),
                    metrics.registryCacheEvictionCount(),
                    metrics.registryCacheSize(),
                    metrics.registryCacheLimit(),
                    metrics.segmentCacheKeyLimitPerSegment(),
                    metrics.maxNumberOfKeysInSegmentWriteCache(),
                    metrics.maxNumberOfKeysInSegmentWriteCacheDuringMaintenance(),
                    metrics.segmentCount(), metrics.segmentReadyCount(),
                    metrics.segmentMaintenanceCount(),
                    metrics.segmentErrorCount(),
                    metrics.segmentClosedCount(), metrics.segmentBusyCount(),
                    metrics.totalSegmentKeys(),
                    metrics.totalSegmentCacheKeys(),
                    metrics.totalWriteCacheKeys(),
                    metrics.totalDeltaCacheFiles(),
                    compactRequestCount,
                    flushRequestCount,
                    metrics.splitScheduleCount(),
                    metrics.splitInFlightCount(),
                    metrics.maintenanceQueueSize(),
                    metrics.maintenanceQueueCapacity(),
                    metrics.splitQueueSize(),
                    metrics.splitQueueCapacity(),
                    metrics.readLatencyP50Micros(),
                    metrics.readLatencyP95Micros(),
                    metrics.readLatencyP99Micros(),
                    metrics.writeLatencyP50Micros(),
                    metrics.writeLatencyP95Micros(),
                    metrics.writeLatencyP99Micros(),
                    metrics.bloomFilterHashFunctions(),
                    metrics.bloomFilterIndexSizeInBytes(),
                    metrics.bloomFilterProbabilityOfFalsePositive(),
                    metrics.bloomFilterRequestCount(),
                    metrics.bloomFilterRefusedCount(),
                    metrics.bloomFilterPositiveCount(),
                    metrics.bloomFilterFalsePositiveCount(),
                    metrics.jvmHeapUsedBytes(),
                    metrics.jvmHeapCommittedBytes(),
                    metrics.jvmNonHeapUsedBytes(),
                    metrics.jvmGcCount(), metrics.jvmGcTimeMillis(),
                    Duration.ofNanos(System.nanoTime() - startedNanos)
                            .toMillis(),
                    Instant.now(), "");
        } catch (final Exception e) {
            addEvent("NODE_POLL_FAILED", node.nodeId(),
                    "Polling failed: " + e.getMessage());
            return unavailable(node, e.getMessage(), startedNanos);
        }
    }

    private NodeDashboardEntry unavailable(final RegisteredNode node,
            final String error, final long startedNanos) {
        return new NodeDashboardEntry(node.nodeId(), node.nodeName(),
                node.baseUrl(), false, "UNAVAILABLE", false,
                0L, 0L, 0L,
                0L, 0L, 0L, 0L,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0L, 0L, 0L, 0L,
                0L, 0L, 0L,
                0, 0, 0, 0, 0,
                0L, 0L, 0L, 0L, 0L, 0L,
                0, 0, 0D,
                0L, 0L, 0L, 0L,
                0L, 0L, 0L, 0L, 0L,
                Duration.ofNanos(System.nanoTime() - startedNanos).toMillis(),
                Instant.now(), error == null ? "" : error);
    }

    private long getActionRequestCount(final String nodeId,
            final ActionType actionType) {
        final LongAdder counter = actionRequestCounters
                .get(actionCounterKey(nodeId, actionType));
        return counter == null ? 0L : counter.sum();
    }

    private String actionCounterKey(final String nodeId,
            final ActionType actionType) {
        return nodeId + ":" + actionType.name();
    }

    private HttpResponse<String> sendGet(final String url, final String token)
            throws IOException, InterruptedException {
        final HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url)).timeout(Duration.ofSeconds(3));
        if (token != null && !token.isBlank()) {
            builder.header("Authorization", "Bearer " + token.trim());
        }
        final HttpRequest request = builder.GET().build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private <T> T readRequest(final HttpExchange exchange,
            final Class<T> requestType) throws IOException {
        try {
            final byte[] body = exchange.getRequestBody().readAllBytes();
            return objectMapper.readValue(body, requestType);
        } catch (final RuntimeException e) {
            writeError(exchange, 400, "INVALID_REQUEST",
                    "Invalid JSON payload.", "");
            throw new RequestHandledException();
        }
    }

    private String normalizeBaseUrl(final String baseUrl) {
        final URI uri = URI.create(Objects.requireNonNull(baseUrl, "baseUrl"));
        final String value = uri.toString().trim();
        if (!value.startsWith("http://") && !value.startsWith("https://")) {
            throw new IllegalArgumentException("baseUrl must use http(s)");
        }
        if (requireTlsToNodes && !value.startsWith("https://")) {
            throw new IllegalArgumentException(
                    "baseUrl must use https when TLS is required");
        }
        if (value.endsWith("/")) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }

    private String normalizeOptional(final String value) {
        if (value == null) {
            return "";
        }
        return value.trim();
    }

    private void backoff(final int attempt) throws InterruptedException {
        final long millis = Math.min(400L, 50L << (attempt - 1));
        Thread.sleep(millis);
    }

    private boolean requireWriteAccess(final HttpExchange exchange)
            throws IOException {
        if (writeToken.isEmpty()) {
            return true;
        }
        final String incoming = exchange.getRequestHeaders()
                .getFirst("X-Hestia-Console-Token");
        if (!writeToken.equals(incoming)) {
            writeError(exchange, 403, "FORBIDDEN",
                    "Missing or invalid console write token.", "");
            return false;
        }
        return true;
    }

    private void addEvent(final String type, final String nodeId,
            final String detail) {
        events.addFirst(new ConsoleEvent(Instant.now(), type, nodeId, detail));
        while (events.size() > MAX_EVENTS) {
            events.removeLast();
        }
    }

    private void writeMethodNotAllowed(final HttpExchange exchange)
            throws IOException {
        writeError(exchange, 405, "METHOD_NOT_ALLOWED",
                "HTTP method is not allowed.", "");
    }

    private void writeError(final HttpExchange exchange, final int statusCode,
            final String code, final String message, final String requestId)
            throws IOException {
        writeJson(exchange, statusCode,
                new ErrorResponse(code, message, requestId, Instant.now()));
    }

    private void writeNoContent(final HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(204, -1);
        exchange.close();
    }

    private void writeJson(final HttpExchange exchange, final int statusCode,
            final Object payload) throws IOException {
        final byte[] bytes = objectMapper.writeValueAsBytes(payload);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        } finally {
            exchange.close();
        }
    }

    /**
     * Request payload for registering node entries.
     *
     * @param nodeId   unique node id
     * @param nodeName display name
     * @param baseUrl  node agent base URL
     */
    public record NodeRegistrationRequest(String nodeId, String nodeName,
            String baseUrl, String agentToken) {
        /**
         * Validation constructor.
         */
        public NodeRegistrationRequest {
            nodeId = normalize(nodeId, "nodeId");
            nodeName = normalize(nodeName, "nodeName");
            baseUrl = normalize(baseUrl, "baseUrl");
            agentToken = agentToken == null ? "" : agentToken.trim();
        }
    }

    /**
     * Registered node data.
     *
     * @param nodeId       unique node id
     * @param nodeName     display name
     * @param baseUrl      node agent base URL
     * @param registeredAt registration timestamp
     */
    public record RegisteredNode(String nodeId, String nodeName, String baseUrl,
            String agentToken, Instant registeredAt) {
        /**
         * Validation constructor.
         */
        public RegisteredNode {
            nodeId = normalize(nodeId, "nodeId");
            nodeName = normalize(nodeName, "nodeName");
            baseUrl = normalize(baseUrl, "baseUrl");
            agentToken = agentToken == null ? "" : agentToken.trim();
            registeredAt = Objects.requireNonNull(registeredAt, "registeredAt");
        }
    }

    /**
     * Aggregated dashboard entry for one managed node.
     *
     * @param nodeId               unique node id
     * @param nodeName             display name
     * @param baseUrl              node agent base URL
     * @param reachable            true when last poll succeeded
     * @param state                state string or UNAVAILABLE
     * @param ready                readiness flag
     * @param getOperationCount    get operation count
     * @param putOperationCount    put operation count
     * @param deleteOperationCount delete operation count
     * @param registryCacheHitCount registry cache hit count
     * @param registryCacheMissCount registry cache miss count
     * @param registryCacheLoadCount registry cache load count
     * @param registryCacheEvictionCount registry cache eviction count
     * @param registryCacheSize registry cache size
     * @param registryCacheLimit registry cache size limit
     * @param segmentCacheKeyLimitPerSegment max cache keys per segment
     * @param maxNumberOfKeysInSegmentWriteCache max keys in segment write cache
     * @param maxNumberOfKeysInSegmentWriteCacheDuringMaintenance max keys in segment write cache during maintenance
     * @param segmentCount segment count
     * @param segmentReadyCount number of READY segments
     * @param segmentMaintenanceCount number of maintenance-state segments
     * @param segmentErrorCount number of ERROR segments
     * @param segmentClosedCount number of CLOSED segments
     * @param segmentBusyCount number of unavailable segment snapshots
     * @param totalSegmentKeys total keys across segments
     * @param totalSegmentCacheKeys total keys buffered in segment caches
     * @param totalWriteCacheKeys total keys buffered in write caches
     * @param totalDeltaCacheFiles total delta cache file count
     * @param compactRequestCount compaction request count
     * @param flushRequestCount flush request count
     * @param splitScheduleCount split scheduling count
     * @param splitInFlightCount split in-flight count
     * @param maintenanceQueueSize maintenance queue size
     * @param maintenanceQueueCapacity maintenance queue capacity
     * @param splitQueueSize split queue size
     * @param splitQueueCapacity split queue capacity
     * @param readLatencyP50Micros read latency p50 in microseconds
     * @param readLatencyP95Micros read latency p95 in microseconds
     * @param readLatencyP99Micros read latency p99 in microseconds
     * @param writeLatencyP50Micros write latency p50 in microseconds
     * @param writeLatencyP95Micros write latency p95 in microseconds
     * @param writeLatencyP99Micros write latency p99 in microseconds
     * @param bloomFilterHashFunctions bloom filter hash function count
     * @param bloomFilterIndexSizeInBytes bloom filter size in bytes
     * @param bloomFilterProbabilityOfFalsePositive bloom filter FP probability
     * @param bloomFilterRequestCount bloom filter request count
     * @param bloomFilterRefusedCount bloom filter refused count
     * @param bloomFilterPositiveCount bloom filter positive response count
     * @param bloomFilterFalsePositiveCount bloom filter false-positive count
     * @param jvmHeapUsedBytes JVM heap used bytes
     * @param jvmHeapCommittedBytes JVM heap committed bytes
     * @param jvmNonHeapUsedBytes JVM non-heap used bytes
     * @param jvmGcCount JVM GC count
     * @param jvmGcTimeMillis JVM GC time in milliseconds
     * @param pollLatencyMillis    end-to-end poll latency
     * @param capturedAt           snapshot timestamp
     * @param error                optional poll error detail
     */
    public record NodeDashboardEntry(String nodeId, String nodeName,
            String baseUrl, boolean reachable, String state, boolean ready,
            long getOperationCount, long putOperationCount,
            long deleteOperationCount, long registryCacheHitCount,
            long registryCacheMissCount, long registryCacheLoadCount,
            long registryCacheEvictionCount, int registryCacheSize,
            int registryCacheLimit, int segmentCacheKeyLimitPerSegment,
            int maxNumberOfKeysInSegmentWriteCache,
            int maxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
            int segmentCount, int segmentReadyCount,
            int segmentMaintenanceCount, int segmentErrorCount,
            int segmentClosedCount, int segmentBusyCount, long totalSegmentKeys,
            long totalSegmentCacheKeys, long totalWriteCacheKeys,
            long totalDeltaCacheFiles, long compactRequestCount,
            long flushRequestCount, long splitScheduleCount,
            int splitInFlightCount, int maintenanceQueueSize,
            int maintenanceQueueCapacity, int splitQueueSize,
            int splitQueueCapacity, long readLatencyP50Micros,
            long readLatencyP95Micros, long readLatencyP99Micros,
            long writeLatencyP50Micros, long writeLatencyP95Micros,
            long writeLatencyP99Micros, int bloomFilterHashFunctions,
            int bloomFilterIndexSizeInBytes,
            double bloomFilterProbabilityOfFalsePositive,
            long bloomFilterRequestCount, long bloomFilterRefusedCount,
            long bloomFilterPositiveCount,
            long bloomFilterFalsePositiveCount,
            long jvmHeapUsedBytes, long jvmHeapCommittedBytes,
            long jvmNonHeapUsedBytes, long jvmGcCount, long jvmGcTimeMillis,
            long pollLatencyMillis, Instant capturedAt, String error) {
        /**
         * Validation constructor.
         */
        public NodeDashboardEntry {
            nodeId = normalize(nodeId, "nodeId");
            nodeName = normalize(nodeName, "nodeName");
            baseUrl = normalize(baseUrl, "baseUrl");
            state = normalize(state, "state");
            if (getOperationCount < 0 || putOperationCount < 0
                    || deleteOperationCount < 0
                    || registryCacheHitCount < 0
                    || registryCacheMissCount < 0
                    || registryCacheLoadCount < 0
                    || registryCacheEvictionCount < 0
                    || registryCacheSize < 0 || registryCacheLimit < 0
                    || segmentCacheKeyLimitPerSegment < 0
                    || maxNumberOfKeysInSegmentWriteCache < 0
                    || maxNumberOfKeysInSegmentWriteCacheDuringMaintenance < 0
                    || segmentCount < 0 || segmentReadyCount < 0
                    || segmentMaintenanceCount < 0 || segmentErrorCount < 0
                    || segmentClosedCount < 0 || segmentBusyCount < 0
                    || totalSegmentKeys < 0 || totalSegmentCacheKeys < 0
                    || totalWriteCacheKeys < 0 || totalDeltaCacheFiles < 0
                    || compactRequestCount < 0 || flushRequestCount < 0
                    || splitScheduleCount < 0 || splitInFlightCount < 0
                    || maintenanceQueueSize < 0
                    || maintenanceQueueCapacity < 0 || splitQueueSize < 0
                    || splitQueueCapacity < 0 || readLatencyP50Micros < 0
                    || readLatencyP95Micros < 0 || readLatencyP99Micros < 0
                    || writeLatencyP50Micros < 0
                    || writeLatencyP95Micros < 0
                    || writeLatencyP99Micros < 0
                    || bloomFilterHashFunctions < 0
                    || bloomFilterIndexSizeInBytes < 0
                    || bloomFilterProbabilityOfFalsePositive < 0
                    || bloomFilterRequestCount < 0
                    || bloomFilterRefusedCount < 0
                    || bloomFilterPositiveCount < 0
                    || bloomFilterFalsePositiveCount < 0
                    || jvmHeapUsedBytes < 0 || jvmHeapCommittedBytes < 0
                    || jvmNonHeapUsedBytes < 0 || jvmGcCount < 0
                    || jvmGcTimeMillis < 0
                    || pollLatencyMillis < 0) {
                throw new IllegalArgumentException("counts/latency must be >=0");
            }
            capturedAt = Objects.requireNonNull(capturedAt, "capturedAt");
            error = error == null ? "" : error;
        }
    }

    /**
     * Request payload for flush/compact actions.
     *
     * @param nodeId    target node id
     * @param requestId operation request id
     * @param confirmed explicit user confirmation flag
     */
    public record ConsoleActionRequest(String nodeId, String requestId,
            boolean confirmed) {
        /**
         * Validation constructor.
         */
        public ConsoleActionRequest {
            nodeId = normalize(nodeId, "nodeId");
            requestId = normalize(requestId, "requestId");
        }
    }

    /**
     * Action lifecycle payload.
     *
     * @param actionId   generated action id
     * @param nodeId     node id
     * @param action     action type name
     * @param status     PENDING/SUCCESS/FAILED
     * @param message    optional details
     * @param createdAt  creation timestamp
     * @param updatedAt  last status update timestamp
     */
    public record ConsoleActionStatusResponse(String actionId, String nodeId,
            String action, String status, String message, Instant createdAt,
            Instant updatedAt) {
        /**
         * Validation constructor.
         */
        public ConsoleActionStatusResponse {
            actionId = normalize(actionId, "actionId");
            nodeId = normalize(nodeId, "nodeId");
            action = normalize(action, "action");
            status = normalize(status, "status");
            message = message == null ? "" : message;
            createdAt = Objects.requireNonNull(createdAt, "createdAt");
            updatedAt = Objects.requireNonNull(updatedAt, "updatedAt");
        }
    }

    /**
     * Console event entry for read-side timeline.
     *
     * @param at     event timestamp
     * @param type   event type
     * @param nodeId related node id
     * @param detail event detail message
     */
    public record ConsoleEvent(Instant at, String type, String nodeId,
            String detail) {
        /**
         * Validation constructor.
         */
        public ConsoleEvent {
            at = Objects.requireNonNull(at, "at");
            type = normalize(type, "type");
            nodeId = normalize(nodeId, "nodeId");
            detail = detail == null ? "" : detail;
        }
    }

    private static String normalize(final String value, final String name) {
        final String normalized = Objects.requireNonNull(value, name).trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return normalized;
    }

    @FunctionalInterface
    private interface Handler {
        void handle(HttpExchange exchange) throws Exception;
    }

    private static final class RequestHandledException
            extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
