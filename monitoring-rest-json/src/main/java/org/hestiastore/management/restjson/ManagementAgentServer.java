package org.hestiastore.management.restjson;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.hestiastore.index.monitoring.MonitoredIndex;
import org.hestiastore.index.monitoring.MonitoredIndexProvider;
import org.hestiastore.index.control.model.ConfigurationSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimePatchValidation;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.monitoring.json.api.ActionRequest;
import org.hestiastore.monitoring.json.api.ActionResponse;
import org.hestiastore.monitoring.json.api.ActionStatus;
import org.hestiastore.monitoring.json.api.ActionType;
import org.hestiastore.monitoring.json.api.ConfigPatchRequest;
import org.hestiastore.monitoring.json.api.ConfigViewResponse;
import org.hestiastore.monitoring.json.api.ErrorResponse;
import org.hestiastore.monitoring.json.api.IndexReportResponse;
import org.hestiastore.monitoring.json.api.JvmMetricsResponse;
import org.hestiastore.monitoring.json.api.ManagementApiPaths;
import org.hestiastore.monitoring.json.api.NodeReportResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * Lightweight node-local HTTP management agent.
 */
public final class ManagementAgentServer
        implements AutoCloseable, MonitoredIndexProvider {

    private static final String METHOD_GET = "GET";
    private static final String METHOD_POST = "POST";
    private static final String METHOD_PATCH = "PATCH";
    private static final int MAX_AUDIT_RECORDS = 10_000;
    private static final int MAX_REQUEST_BODY_BYTES = 1_048_576;
    private static final Map<String, RuntimeSettingKey> RUNTIME_KEY_BY_API_NAME = Map
            .of("maxNumberOfSegmentsInCache",
                    RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                    "maxNumberOfKeysInSegmentCache",
                    RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
                    "maxNumberOfKeysInSegmentWriteCache",
                    RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE,
                    "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance",
                    RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE);
    private static final Map<RuntimeSettingKey, String> API_NAME_BY_RUNTIME_KEY = buildApiNameByRuntimeKey();
    private static final List<String> SUPPORTED_RUNTIME_CONFIG_KEYS = RUNTIME_KEY_BY_API_NAME
            .keySet().stream().sorted().toList();

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ConcurrentMap<String, SegmentIndex<?, ?>> indexes = new ConcurrentHashMap<>();
    private final ManagementAgentSecurityPolicy securityPolicy;
    private final FixedWindowRateLimiter mutatingRateLimiter;
    private final ConcurrentLinkedDeque<AuditRecord> auditTrail = new ConcurrentLinkedDeque<>();
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());
    private final HttpServer server;
    private final ExecutorService requestExecutor;

    /**
     * Creates a management agent server.
     *
     * @param bindAddress            host to bind
     * @param bindPort               port to bind (0 means random free port)
     * @param index                  index instance exposed by this agent
     * @param indexName              logical index name returned in responses
     * @param runtimeConfigAllowlist retained for backward compatibility
     * @throws IOException when HTTP server cannot be created
     */
    public ManagementAgentServer(final String bindAddress, final int bindPort,
            final SegmentIndex<?, ?> index, final String indexName,
            final Set<String> runtimeConfigAllowlist) throws IOException {
        this(bindAddress, bindPort, runtimeConfigAllowlist,
                ManagementAgentSecurityPolicy.permissive());
        addIndex(indexName, index);
    }

    /**
     * Creates a management agent server.
     *
     * @param bindAddress            host to bind
     * @param bindPort               port to bind (0 means random free port)
     * @param index                  index instance exposed by this agent
     * @param indexName              logical index name returned in responses
     * @param runtimeConfigAllowlist retained for backward compatibility
     * @param securityPolicy         authn/authz/rate-limit policy
     * @throws IOException when HTTP server cannot be created
     */
    public ManagementAgentServer(final String bindAddress, final int bindPort,
            final SegmentIndex<?, ?> index, final String indexName,
            final Set<String> runtimeConfigAllowlist,
            final ManagementAgentSecurityPolicy securityPolicy)
            throws IOException {
        this(bindAddress, bindPort, runtimeConfigAllowlist, securityPolicy);
        addIndex(indexName, index);
    }

    /**
     * Creates a management agent server without initial indexes.
     *
     * @param bindAddress            host to bind
     * @param bindPort               port to bind (0 means random free port)
     * @param runtimeConfigAllowlist retained for backward compatibility
     * @throws IOException when HTTP server cannot be created
     */
    public ManagementAgentServer(final String bindAddress, final int bindPort,
            final Set<String> runtimeConfigAllowlist) throws IOException {
        this(bindAddress, bindPort, runtimeConfigAllowlist,
                ManagementAgentSecurityPolicy.permissive());
    }

    /**
     * Creates a management agent server without initial indexes.
     *
     * @param bindAddress            host to bind
     * @param bindPort               port to bind (0 means random free port)
     * @param runtimeConfigAllowlist retained for backward compatibility
     * @param securityPolicy         authn/authz/rate-limit policy
     * @throws IOException when HTTP server cannot be created
     */
    public ManagementAgentServer(final String bindAddress, final int bindPort,
            final Set<String> runtimeConfigAllowlist,
            final ManagementAgentSecurityPolicy securityPolicy)
            throws IOException {
        Objects.requireNonNull(runtimeConfigAllowlist, "runtimeConfigAllowlist");
        this.securityPolicy = Objects.requireNonNull(securityPolicy,
                "securityPolicy");
        this.mutatingRateLimiter = new FixedWindowRateLimiter(
                securityPolicy.maxMutatingRequestsPerMinute());
        this.server = HttpServer.create(new InetSocketAddress(
                Objects.requireNonNull(bindAddress, "bindAddress"), bindPort),
                0);
        final int workers = Math.max(2,
                Math.min(16, Runtime.getRuntime().availableProcessors() * 2));
        final AtomicInteger threadCounter = new AtomicInteger(1);
        this.requestExecutor = Executors.newFixedThreadPool(workers,
                runnable -> {
                    final Thread thread = new Thread(runnable,
                            "monitoring-rest-json-http-"
                                    + threadCounter.getAndIncrement());
                    thread.setDaemon(false);
                    return thread;
                });
        this.server.setExecutor(requestExecutor);
        registerRoutes();
    }

    /**
     * Starts serving HTTP requests.
     */
    public void start() {
        server.start();
    }

    /**
     * Returns the bound TCP port.
     *
     * @return local port
     */
    public int getPort() {
        return server.getAddress().getPort();
    }

    /**
     * Registers or replaces monitored index by name.
     *
     * @param indexName logical index name
     * @param index     live index instance
     */
    public void addIndex(final String indexName,
            final SegmentIndex<?, ?> index) {
        indexes.put(normalize(indexName, "indexName"),
                Objects.requireNonNull(index, "index"));
    }

    /**
     * Removes monitored index registration by name.
     *
     * @param indexName logical index name
     * @return true when entry was removed
     */
    public boolean removeIndex(final String indexName) {
        return indexes.remove(normalize(indexName, "indexName")) != null;
    }

    /**
     * Returns names of currently monitored indexes.
     *
     * @return sorted index names
     */
    public List<String> monitoredIndexNames() {
        return activeIndexesSnapshot().stream().map(RegisteredIndex::indexName)
                .toList();
    }

    /**
     * Returns immutable snapshot of mutating endpoint audit records.
     *
     * @return audit snapshot
     */
    public List<AuditRecord> auditTrailSnapshot() {
        return List.copyOf(new ArrayList<>(auditTrail));
    }

    /** {@inheritDoc} */
    @Override
    public List<? extends MonitoredIndex> monitoredIndexes() {
        return List.copyOf(activeIndexesSnapshot());
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        server.stop(0);
        requestExecutor.shutdownNow();
        try {
            requestExecutor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void registerRoutes() {
        server.createContext(ManagementApiPaths.REPORT,
                exchange -> safeHandle(exchange, this::handleReport));
        server.createContext(ManagementApiPaths.ACTION_FLUSH,
                exchange -> safeHandle(exchange, this::handleFlush));
        server.createContext(ManagementApiPaths.ACTION_COMPACT,
                exchange -> safeHandle(exchange, this::handleCompact));
        server.createContext(ManagementApiPaths.CONFIG,
                exchange -> safeHandle(exchange, this::handleConfig));
        server.createContext("/health",
                exchange -> safeHandle(exchange, this::handleHealth));
        server.createContext("/ready",
                exchange -> safeHandle(exchange, this::handleReady));
    }

    private void safeHandle(final HttpExchange exchange, final Handler handler)
            throws IOException {
        try {
            handler.handle(exchange);
        } catch (final Exception e) {
            logger.error("Unhandled management agent error: method={} path={}",
                    exchange.getRequestMethod(), exchange.getRequestURI(), e);
            final ErrorResponse error = new ErrorResponse("INTERNAL_ERROR",
                    "Unexpected management agent failure.", "", Instant.now());
            writeJson(exchange, 500, error);
        }
    }

    private void handleReport(final HttpExchange exchange) throws IOException {
        if (!METHOD_GET.equals(exchange.getRequestMethod())) {
            writeMethodNotAllowed(exchange);
            return;
        }
        if (!authorize(exchange, AgentRole.READ, false,
                ManagementApiPaths.REPORT, "")) {
            return;
        }
        final Instant capturedAt = Instant.now();
        final List<IndexReportResponse> indexSections = activeIndexesSnapshot()
                .stream().map(this::toIndexReport).toList();
        final NodeReportResponse response = new NodeReportResponse(
                readJvmMetrics(), indexSections, capturedAt);
        writeJson(exchange, 200, response);
    }

    private void handleFlush(final HttpExchange exchange) throws IOException {
        handleMutatingAction(exchange, ActionType.FLUSH,
                registered -> registered.index().flushAndWait());
    }

    private void handleCompact(final HttpExchange exchange) throws IOException {
        handleMutatingAction(exchange, ActionType.COMPACT,
                registered -> registered.index().compactAndWait());
    }

    private void handleConfig(final HttpExchange exchange) throws IOException {
        final String endpoint = ManagementApiPaths.CONFIG;
        final String method = exchange.getRequestMethod();
        if (METHOD_GET.equals(method)) {
            handleConfigGet(exchange, endpoint);
            return;
        }
        if (METHOD_PATCH.equals(method)) {
            handleConfigPatch(exchange, endpoint);
            return;
        }
        writeMethodNotAllowed(exchange);
    }

    private void handleConfigGet(final HttpExchange exchange,
            final String endpoint) throws IOException {
        if (!authorize(exchange, AgentRole.READ, false, endpoint, "")) {
            return;
        }
        final Optional<String> oIndexName = queryParam(exchange, "indexName");
        if (oIndexName.isEmpty()) {
            writeError(exchange, 400, "INVALID_REQUEST",
                    "Query parameter 'indexName' is required.", "");
            return;
        }
        final RegisteredIndex target = findRegisteredIndex(oIndexName.get())
                .orElse(null);
        if (target == null) {
            writeError(exchange, 404, "INDEX_NOT_FOUND",
                    "Unknown index: " + oIndexName.get(), "");
            return;
        }
        final ConfigViewResponse response = toConfigViewResponse(target);
        writeJson(exchange, 200, response);
    }

    private void handleConfigPatch(final HttpExchange exchange,
            final String endpoint) throws IOException {
        final String body;
        try {
            body = readBody(exchange);
        } catch (final RequestBodyTooLargeException e) {
            writeError(exchange, 413, "REQUEST_TOO_LARGE", e.getMessage(), "");
            audit(exchange, endpoint, "", 413, "REJECTED_TOO_LARGE");
            return;
        }
        if (!authorize(exchange, AgentRole.ADMIN, true, endpoint, body)) {
            return;
        }
        final ConfigPatchRequest request;
        try {
            request = objectMapper.readValue(body, ConfigPatchRequest.class);
        } catch (final RuntimeException e) {
            final ErrorResponse error = new ErrorResponse("INVALID_REQUEST",
                    "Invalid JSON payload.", "", Instant.now());
            writeJson(exchange, 400, error);
            audit(exchange, endpoint, body, 400, "FAILED");
            return;
        }
        final Optional<String> oIndexName = queryParam(exchange, "indexName");
        if (oIndexName.isEmpty()) {
            writeError(exchange, 400, "INVALID_REQUEST",
                    "Query parameter 'indexName' is required.", "");
            audit(exchange, endpoint, body, 400, "REJECTED");
            return;
        }
        final String indexName = oIndexName.get();
        final RegisteredIndex target = findRegisteredIndex(indexName)
                .orElse(null);
        if (target == null) {
            final ErrorResponse error = new ErrorResponse("INDEX_NOT_FOUND",
                    "Unknown index: " + indexName, "", Instant.now());
            writeJson(exchange, 404, error);
            audit(exchange, endpoint, body, 404, "REJECTED");
            return;
        }
        if (!target.ready()) {
            final ErrorResponse error = new ErrorResponse("INVALID_STATE",
                    "Index is not in READY state: " + indexName, "",
                    Instant.now());
            writeJson(exchange, 409, error);
            audit(exchange, endpoint, body, 409, "REJECTED");
            return;
        }
        final RuntimeConfigPatch runtimePatch;
        try {
            runtimePatch = toRuntimePatch(request);
        } catch (final ConfigKeyNotSupportedException e) {
            final ErrorResponse error = new ErrorResponse(
                    "CONFIG_KEY_NOT_SUPPORTED", e.getMessage(), "",
                    Instant.now());
            writeJson(exchange, 400, error);
            audit(exchange, endpoint, body, 400, "REJECTED");
            return;
        } catch (final IllegalArgumentException e) {
            final ErrorResponse error = new ErrorResponse("INVALID_REQUEST",
                    e.getMessage(), "", Instant.now());
            writeJson(exchange, 400, error);
            audit(exchange, endpoint, body, 400, "REJECTED");
            return;
        }
        final RuntimePatchResult result = target.index().controlPlane()
                .configuration().apply(runtimePatch);
        if (!result.validation().valid()) {
            final ErrorResponse error = new ErrorResponse("INVALID_REQUEST",
                    formatValidationIssues(result.validation()), "",
                    Instant.now());
            writeJson(exchange, 400, error);
            audit(exchange, endpoint, body, 400, "REJECTED");
            return;
        }
        if (!request.dryRun() && !result.applied()) {
            final ErrorResponse error = new ErrorResponse("INVALID_STATE",
                    "Runtime patch was not applied.", "", Instant.now());
            writeJson(exchange, 409, error);
            audit(exchange, endpoint, body, 409, "REJECTED");
            return;
        }
        writeNoContent(exchange);
        audit(exchange, endpoint, body, 204,
                request.dryRun() ? "DRY_RUN" : "APPLIED");
    }

    private void handleHealth(final HttpExchange exchange) throws IOException {
        if (!METHOD_GET.equals(exchange.getRequestMethod())) {
            writeMethodNotAllowed(exchange);
            return;
        }
        if (!authorize(exchange, AgentRole.READ, false, "/health", "")) {
            return;
        }
        writeJson(exchange, 200, "{\"status\":\"UP\"}");
    }

    private void handleReady(final HttpExchange exchange) throws IOException {
        if (!METHOD_GET.equals(exchange.getRequestMethod())) {
            writeMethodNotAllowed(exchange);
            return;
        }
        if (!authorize(exchange, AgentRole.READ, false, "/ready", "")) {
            return;
        }
        final List<RegisteredIndex> active = activeIndexesSnapshot();
        if (!active.isEmpty() && notReadyIndexes(active).isEmpty()) {
            writeJson(exchange, 200, "{\"status\":\"READY\"}");
        } else {
            writeJson(exchange, 503, "{\"status\":\"NOT_READY\"}");
        }
    }

    private void handleMutatingAction(final HttpExchange exchange,
            final ActionType action, final IndexOperation operation)
            throws IOException {
        final String endpoint = exchange.getRequestURI().getPath();
        if (!METHOD_POST.equals(exchange.getRequestMethod())) {
            writeMethodNotAllowed(exchange);
            return;
        }
        final String body;
        try {
            body = readBody(exchange);
        } catch (final RequestBodyTooLargeException e) {
            writeError(exchange, 413, "REQUEST_TOO_LARGE", e.getMessage(), "");
            audit(exchange, endpoint, "", 413, "REJECTED_TOO_LARGE");
            return;
        }
        if (!authorize(exchange, AgentRole.OPERATE, true, endpoint, body)) {
            return;
        }
        final ActionRequest request;
        try {
            request = objectMapper.readValue(body, ActionRequest.class);
        } catch (final RuntimeException e) {
            final ErrorResponse error = new ErrorResponse("INVALID_REQUEST",
                    "Invalid JSON payload.", "", Instant.now());
            writeJson(exchange, 400, error);
            audit(exchange, endpoint, body, 400, "FAILED");
            return;
        }
        final List<RegisteredIndex> targets;
        try {
            targets = resolveActionTargets(request);
        } catch (final IllegalStateException e) {
            final ErrorResponse error = new ErrorResponse("INDEX_NOT_FOUND",
                    e.getMessage(), request.requestId(), Instant.now());
            writeJson(exchange, 404, error);
            audit(exchange, endpoint, body, 404, "REJECTED");
            return;
        }
        if (targets.isEmpty()) {
            final ErrorResponse error = new ErrorResponse("INVALID_STATE",
                    "No monitored indexes available.", request.requestId(),
                    Instant.now());
            writeJson(exchange, 409, error);
            audit(exchange, endpoint, body, 409, "REJECTED");
            return;
        }
        final List<String> notReady = notReadyIndexes(targets);
        if (!notReady.isEmpty()) {
            final ErrorResponse error = new ErrorResponse("INVALID_STATE",
                    "Indexes are not in READY state: "
                            + String.join(",", notReady),
                    request.requestId(), Instant.now());
            writeJson(exchange, 409, error);
            audit(exchange, endpoint, body, 409, "REJECTED");
            return;
        }
        try {
            for (final RegisteredIndex target : targets) {
                operation.run(target);
            }
            final ActionResponse response = new ActionResponse(
                    request.requestId(), action, ActionStatus.COMPLETED,
                    "Applied to " + targets.size() + " index(es).",
                    Instant.now());
            writeJson(exchange, 200, response);
            audit(exchange, endpoint, body, 200, "COMPLETED");
        } catch (final RuntimeException e) {
            final ActionResponse response = new ActionResponse(
                    request.requestId(), action, ActionStatus.FAILED,
                    e.getMessage(), Instant.now());
            writeJson(exchange, 500, response);
            audit(exchange, endpoint, body, 500, "FAILED");
        }
    }

    private List<RegisteredIndex> resolveActionTargets(
            final ActionRequest request) {
        final List<RegisteredIndex> active = activeIndexesSnapshot();
        if (request.indexName() == null) {
            return active;
        }
        return active.stream()
                .filter(i -> i.indexName().equals(request.indexName()))
                .findFirst().map(List::of)
                .orElseThrow(() -> new IllegalStateException(
                        "Unknown index: " + request.indexName()));
    }

    private List<RegisteredIndex> activeIndexesSnapshot() {
        final List<RegisteredIndex> active = new ArrayList<>();
        for (final java.util.Map.Entry<String, SegmentIndex<?, ?>> entry : indexes
                .entrySet()) {
            final String name = entry.getKey();
            final SegmentIndex<?, ?> index = entry.getValue();
            if (isClosed(name, index)) {
                indexes.remove(name, index);
                continue;
            }
            active.add(new RegisteredIndex(name, index));
        }
        active.sort(Comparator.comparing(RegisteredIndex::indexName));
        return List.copyOf(active);
    }

    private boolean isClosed(final String indexName,
            final SegmentIndex<?, ?> index) {
        try {
            if (index.wasClosed()) {
                logger.info("Removing closed index from reporting: {}",
                        indexName);
                return true;
            }
            final SegmentIndexState state = index.getState();
            if (state == SegmentIndexState.CLOSED) {
                logger.info("Removing CLOSED index from reporting: {}",
                        indexName);
                return true;
            }
            return false;
        } catch (final RuntimeException e) {
            logger.warn("Removing index after state read failure: {}",
                    indexName, e);
            return true;
        }
    }

    private List<String> notReadyIndexes(final List<RegisteredIndex> targets) {
        return targets.stream().filter(target -> !target.ready())
                .map(RegisteredIndex::indexName).collect(Collectors.toList());
    }

    private JvmMetricsResponse readJvmMetrics() {
        final MemoryMXBean memoryMxBean = ManagementFactory.getMemoryMXBean();
        final MemoryUsage heap = memoryMxBean.getHeapMemoryUsage();
        final MemoryUsage nonHeap = memoryMxBean.getNonHeapMemoryUsage();
        long gcCount = 0L;
        long gcTimeMillis = 0L;
        for (final GarbageCollectorMXBean gcMxBean : ManagementFactory
                .getGarbageCollectorMXBeans()) {
            if (gcMxBean.getCollectionCount() > 0) {
                gcCount += gcMxBean.getCollectionCount();
            }
            if (gcMxBean.getCollectionTime() > 0) {
                gcTimeMillis += gcMxBean.getCollectionTime();
            }
        }
        return new JvmMetricsResponse(Math.max(0L, heap.getUsed()),
                Math.max(0L, heap.getCommitted()),
                Math.max(0L, Runtime.getRuntime().maxMemory()),
                Math.max(0L, nonHeap.getUsed()), gcCount, gcTimeMillis);
    }

    private IndexReportResponse toIndexReport(final RegisteredIndex monitored) {
        final SegmentIndexMetricsSnapshot snapshot = monitored
                .metricsSnapshot();
        final SegmentIndexState state = monitored.state();
        return new IndexReportResponse(monitored.indexName(), state.name(),
                state == SegmentIndexState.READY,
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
                snapshot.getSegmentCount(), snapshot.getSegmentReadyCount(),
                snapshot.getSegmentMaintenanceCount(),
                snapshot.getSegmentErrorCount(),
                snapshot.getSegmentClosedCount(),
                snapshot.getSegmentBusyCount(), snapshot.getTotalSegmentKeys(),
                snapshot.getTotalSegmentCacheKeys(),
                snapshot.getTotalWriteCacheKeys(),
                snapshot.getTotalDeltaCacheFiles(),
                snapshot.getCompactRequestCount(),
                snapshot.getFlushRequestCount(),
                snapshot.getSplitScheduleCount(),
                snapshot.getSplitInFlightCount(),
                snapshot.getMaintenanceQueueSize(),
                snapshot.getMaintenanceQueueCapacity(),
                snapshot.getSplitQueueSize(), snapshot.getSplitQueueCapacity(),
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
                snapshot.getBloomFilterFalsePositiveCount());
    }

    private ConfigViewResponse toConfigViewResponse(
            final RegisteredIndex monitored) {
        final ConfigurationSnapshot currentConfig = monitored.index()
                .controlPlane().configuration().getConfigurationActual();
        final ConfigurationSnapshot originalConfig = monitored.index()
                .controlPlane().configuration().getConfigurationOriginal();
        return new ConfigViewResponse(monitored.indexName(),
                toApiConfigMap(originalConfig.values()),
                toApiConfigMap(currentConfig.values()),
                supportedRuntimeConfigKeys(),
                currentConfig.revision(), Instant.now());
    }

    private RuntimeConfigPatch toRuntimePatch(
            final ConfigPatchRequest request) {
        final EnumMap<RuntimeSettingKey, Integer> runtimeValues = new EnumMap<>(
                RuntimeSettingKey.class);
        for (final Map.Entry<String, String> entry : request.values()
                .entrySet()) {
            final String key = entry.getKey();
            final RuntimeSettingKey runtimeKey = RUNTIME_KEY_BY_API_NAME
                    .get(key);
            if (runtimeKey == null) {
                throw new ConfigKeyNotSupportedException("Config key '" + key
                        + "' is not supported for runtime tuning.");
            }
            final int value;
            try {
                value = Integer.parseInt(entry.getValue().trim());
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Config key '" + key + "' requires integer value.");
            }
            runtimeValues.put(runtimeKey, Integer.valueOf(value));
        }
        return new RuntimeConfigPatch(runtimeValues, request.dryRun(), null);
    }

    private static List<String> supportedRuntimeConfigKeys() {
        return SUPPORTED_RUNTIME_CONFIG_KEYS;
    }

    private String formatValidationIssues(
            final RuntimePatchValidation validation) {
        return validation.issues().stream().map(issue -> {
            if (issue.key() == null) {
                return issue.message();
            }
            return apiName(issue.key()) + ": " + issue.message();
        }).collect(Collectors.joining("; "));
    }

    private Optional<RegisteredIndex> findRegisteredIndex(
            final String indexName) {
        final String normalizedName = normalize(indexName, "indexName");
        return activeIndexesSnapshot().stream().filter(
                registered -> registered.indexName().equals(normalizedName))
                .findFirst();
    }

    private Optional<String> queryParam(final HttpExchange exchange,
            final String key) {
        final String rawQuery = exchange.getRequestURI().getRawQuery();
        if (rawQuery == null || rawQuery.isBlank()) {
            return Optional.empty();
        }
        for (final String queryPart : rawQuery.split("&")) {
            if (queryPart == null || queryPart.isEmpty()) {
                continue;
            }
            final int equalsIndex = queryPart.indexOf('=');
            final String rawKey = equalsIndex >= 0
                    ? queryPart.substring(0, equalsIndex)
                    : queryPart;
            final String decodedKey = URLDecoder.decode(rawKey,
                    StandardCharsets.UTF_8);
            if (!key.equals(decodedKey)) {
                continue;
            }
            final String rawValue = equalsIndex >= 0
                    ? queryPart.substring(equalsIndex + 1)
                    : "";
            final String decodedValue = URLDecoder
                    .decode(rawValue, StandardCharsets.UTF_8).trim();
            if (decodedValue.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(decodedValue);
        }
        return Optional.empty();
    }

    private static Map<String, Integer> toApiConfigMap(
            final Map<RuntimeSettingKey, Integer> runtimeValues) {
        final LinkedHashMap<String, Integer> values = new LinkedHashMap<>();
        for (final Map.Entry<RuntimeSettingKey, String> entry : API_NAME_BY_RUNTIME_KEY
                .entrySet()) {
            final Integer value = runtimeValues.get(entry.getKey());
            if (value == null) {
                continue;
            }
            values.put(entry.getValue(), value);
        }
        return Map.copyOf(values);
    }

    private static String apiName(final RuntimeSettingKey runtimeKey) {
        final String name = API_NAME_BY_RUNTIME_KEY.get(runtimeKey);
        if (name != null) {
            return name;
        }
        return runtimeKey.name();
    }

    private void writeMethodNotAllowed(final HttpExchange exchange)
            throws IOException {
        final ErrorResponse error = new ErrorResponse("METHOD_NOT_ALLOWED",
                "HTTP method is not allowed.", "", Instant.now());
        writeJson(exchange, 405, error);
    }

    private String readBody(final HttpExchange exchange) throws IOException {
        final byte[] buffer = new byte[8_192];
        int total = 0;
        try (InputStream input = exchange.getRequestBody()) {
            int read;
            final java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
            while ((read = input.read(buffer)) != -1) {
                total += read;
                if (total > MAX_REQUEST_BODY_BYTES) {
                    throw new RequestBodyTooLargeException(
                            "Request body exceeds " + MAX_REQUEST_BODY_BYTES
                                    + " bytes.");
                }
                output.write(buffer, 0, read);
            }
            return output.toString(StandardCharsets.UTF_8);
        }
    }

    private void writeNoContent(final HttpExchange exchange)
            throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(204, -1);
        exchange.close();
    }

    private void writeJson(final HttpExchange exchange, final int statusCode,
            final Object payload) throws IOException {
        final byte[] bytes;
        if (payload instanceof String literalJson) {
            bytes = literalJson.getBytes(StandardCharsets.UTF_8);
        } else {
            bytes = objectMapper.writeValueAsBytes(payload);
        }
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        } finally {
            exchange.close();
        }
    }

    private void audit(final HttpExchange exchange, final String endpoint,
            final String requestBody, final int statusCode,
            final String outcome) {
        final String actor = exchange.getRemoteAddress() == null ? "unknown"
                : exchange.getRemoteAddress().toString();
        final String digest = sha256Hex(requestBody);
        auditTrail.addFirst(new AuditRecord(actor, endpoint, digest, outcome,
                statusCode, Instant.now()));
        while (auditTrail.size() > MAX_AUDIT_RECORDS) {
            auditTrail.pollLast();
        }
        logger.info(
                "audit endpoint={} actor={} status={} outcome={} digest={} timestamp={}",
                endpoint, actor, statusCode, outcome, digest, Instant.now());
    }

    private boolean authorize(final HttpExchange exchange,
            final AgentRole requiredRole, final boolean mutating,
            final String endpoint, final String requestBody)
            throws IOException {
        if (securityPolicy.requireTls() && !isSecureTransport(exchange)) {
            writeError(exchange, 400, "TLS_REQUIRED",
                    "HTTPS transport is required.", "");
            if (mutating) {
                audit(exchange, endpoint, requestBody, 400, "REJECTED_TLS");
            }
            return false;
        }

        final AgentRole actualRole;
        final String actor;
        if (securityPolicy.tokenRoles().isEmpty()) {
            actualRole = AgentRole.ADMIN;
            actor = "anonymous";
        } else {
            final String token = readToken(exchange);
            if (token == null) {
                writeError(exchange, 401, "UNAUTHORIZED",
                        "Authentication token is required.", "");
                if (mutating) {
                    audit(exchange, endpoint, requestBody, 401,
                            "REJECTED_UNAUTHORIZED");
                }
                return false;
            }
            actualRole = securityPolicy.tokenRoles().get(token);
            if (actualRole == null) {
                writeError(exchange, 401, "UNAUTHORIZED",
                        "Authentication token is invalid.", "");
                if (mutating) {
                    audit(exchange, endpoint, requestBody, 401,
                            "REJECTED_UNAUTHORIZED");
                }
                return false;
            }
            actor = token;
        }
        if (!actualRole.allows(requiredRole)) {
            writeError(exchange, 403, "FORBIDDEN",
                    "Insufficient privileges for endpoint.", "");
            if (mutating) {
                audit(exchange, endpoint, requestBody, 403,
                        "REJECTED_FORBIDDEN");
            }
            return false;
        }
        if (mutating
                && !mutatingRateLimiter.tryAcquire(actor + ":" + endpoint)) {
            writeError(exchange, 429, "RATE_LIMITED",
                    "Mutating request rate limit exceeded.", "");
            audit(exchange, endpoint, requestBody, 429, "REJECTED_RATE_LIMIT");
            return false;
        }
        return true;
    }

    private boolean isSecureTransport(final HttpExchange exchange) {
        final String forwardedProto = exchange.getRequestHeaders()
                .getFirst("X-Forwarded-Proto");
        if (forwardedProto != null
                && "https".equalsIgnoreCase(forwardedProto)) {
            return true;
        }
        return exchange instanceof com.sun.net.httpserver.HttpsExchange;
    }

    private String readToken(final HttpExchange exchange) {
        final String auth = exchange.getRequestHeaders()
                .getFirst("Authorization");
        if (auth != null && auth.startsWith("Bearer ")) {
            final String token = auth.substring("Bearer ".length()).trim();
            return token.isEmpty() ? null : token;
        }
        final String legacyHeader = exchange.getRequestHeaders()
                .getFirst("X-Hestia-Agent-Token");
        if (legacyHeader == null || legacyHeader.isBlank()) {
            return null;
        }
        return legacyHeader.trim();
    }

    private void writeError(final HttpExchange exchange, final int statusCode,
            final String code, final String message, final String requestId)
            throws IOException {
        final ErrorResponse error = new ErrorResponse(code, message, requestId,
                Instant.now());
        writeJson(exchange, statusCode, error);
    }

    private String sha256Hex(final String payload) {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            final byte[] data = payload == null ? new byte[0]
                    : payload.getBytes(StandardCharsets.UTF_8);
            return HexFormat.of().formatHex(digest.digest(data));
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private static Map<RuntimeSettingKey, String> buildApiNameByRuntimeKey() {
        final EnumMap<RuntimeSettingKey, String> values = new EnumMap<>(
                RuntimeSettingKey.class);
        for (final Map.Entry<String, RuntimeSettingKey> entry : RUNTIME_KEY_BY_API_NAME
                .entrySet()) {
            values.put(entry.getValue(), entry.getKey());
        }
        return Map.copyOf(values);
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

    @FunctionalInterface
    private interface IndexOperation {
        void run(RegisteredIndex index);
    }

    private static final class RequestBodyTooLargeException
            extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private RequestBodyTooLargeException(final String message) {
            super(message);
        }
    }

    private static final class ConfigKeyNotSupportedException
            extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;

        private ConfigKeyNotSupportedException(final String message) {
            super(message);
        }
    }

    private record RegisteredIndex(String indexName, SegmentIndex<?, ?> index)
            implements MonitoredIndex {

        @Override
        public SegmentIndexState state() {
            return index.getState();
        }

        @Override
        public SegmentIndexMetricsSnapshot metricsSnapshot() {
            return index.metricsSnapshot();
        }
    }

    /**
     * Immutable mutating-endpoint audit record.
     *
     * @param actor      caller token/identity
     * @param endpoint   endpoint path
     * @param digest     SHA-256 request digest
     * @param result     audit result label
     * @param statusCode HTTP status code
     * @param timestamp  record timestamp
     */
    public record AuditRecord(String actor, String endpoint, String digest,
            String result, int statusCode, Instant timestamp) {
    }
}
