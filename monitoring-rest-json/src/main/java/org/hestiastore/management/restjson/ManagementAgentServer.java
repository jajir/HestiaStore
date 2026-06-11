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
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.IndexRuntimeSnapshot;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexExecutorMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexSegmentRuntimeMetrics;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningField;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningPatch;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningPatchBuilder;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningResult;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningSnapshot;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningValidation;
import org.hestiastore.monitoring.json.api.ActionRequest;
import org.hestiastore.monitoring.json.api.ActionResponse;
import org.hestiastore.monitoring.json.api.ActionStatus;
import org.hestiastore.monitoring.json.api.ActionType;
import org.hestiastore.monitoring.json.api.BloomFilterReportResponse;
import org.hestiastore.monitoring.json.api.ChunkStoreCacheReportResponse;
import org.hestiastore.monitoring.json.api.ConfigPatchRequest;
import org.hestiastore.monitoring.json.api.ConfigViewResponse;
import org.hestiastore.monitoring.json.api.ErrorResponse;
import org.hestiastore.monitoring.json.api.ExecutorReportResponse;
import org.hestiastore.monitoring.json.api.IndexReportResponse;
import org.hestiastore.monitoring.json.api.JvmMetricsResponse;
import org.hestiastore.monitoring.json.api.LatencyReportResponse;
import org.hestiastore.monitoring.json.api.ManagementApiPaths;
import org.hestiastore.monitoring.json.api.MaintenanceReportResponse;
import org.hestiastore.monitoring.json.api.NodeReportResponse;
import org.hestiastore.monitoring.json.api.OperationReportResponse;
import org.hestiastore.monitoring.json.api.RegistryCacheReportResponse;
import org.hestiastore.monitoring.json.api.SegmentReportResponse;
import org.hestiastore.monitoring.json.api.SegmentRuntimeReportResponse;
import org.hestiastore.monitoring.json.api.SplitReportResponse;
import org.hestiastore.monitoring.json.api.WalReportResponse;
import org.hestiastore.monitoring.json.api.WritePathReportResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * Lightweight node-local HTTP management agent.
 */
@SuppressWarnings("java:S3776")
public final class ManagementAgentServer
        implements AutoCloseable, MonitoredIndexProvider {

    private static final String METHOD_GET = "GET";
    private static final String METHOD_POST = "POST";
    private static final String METHOD_PATCH = "PATCH";
    private static final String PARAM_INDEX_NAME = "indexName";
    private static final String ERROR_INVALID_REQUEST = "INVALID_REQUEST";
    private static final String ERROR_INDEX_NOT_FOUND = "INDEX_NOT_FOUND";
    private static final String ERROR_INVALID_STATE = "INVALID_STATE";
    private static final String UNKNOWN_INDEX_PREFIX = "Unknown index: ";
    private static final String AUDIT_REJECTED = "REJECTED";
    private static final String AUDIT_FAILED = "FAILED";
    private static final int MAX_AUDIT_RECORDS = 10_000;
    private static final int MAX_ACTION_REPLAYS = 10_000;
    private static final int MAX_REQUEST_BODY_BYTES = 1_048_576;
    private static final Map<RuntimeTuningField, String> API_NAME_BY_RUNTIME_FIELD = Map
            .of(RuntimeTuningField.SEGMENT_CACHED_SEGMENT_LIMIT,
                    "maxNumberOfSegmentsInCache",
                    RuntimeTuningField.SEGMENT_CACHE_KEY_LIMIT,
                    "maxNumberOfKeysInSegmentCache",
                    RuntimeTuningField.WRITE_PATH_SEGMENT_WRITE_CACHE_KEY_LIMIT,
                    "segmentWriteCacheKeyLimit",
                    RuntimeTuningField.WRITE_PATH_SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE,
                    "segmentWriteCacheKeyLimitDuringMaintenance",
                    RuntimeTuningField.WRITE_PATH_INDEX_BUFFERED_WRITE_KEY_LIMIT,
                    "indexBufferedWriteKeyLimit",
                    RuntimeTuningField.WRITE_PATH_SEGMENT_SPLIT_KEY_THRESHOLD,
                    "segmentSplitKeyThreshold",
                    RuntimeTuningField.CHUNK_STORE_CACHE_PAGE_LIMIT,
                    "chunkStoreCachePageLimit");
    private static final Map<String, RuntimeTuningField> RUNTIME_FIELD_BY_API_NAME = buildRuntimeFieldByApiName();
    private static final List<String> SUPPORTED_RUNTIME_CONFIG_KEYS = API_NAME_BY_RUNTIME_FIELD
            .values().stream().sorted().toList();

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ConcurrentMap<String, SegmentIndex<?, ?>> indexes = new ConcurrentHashMap<>();
    private final ManagementAgentSecurityPolicy securityPolicy;
    private final FixedWindowRateLimiter mutatingRateLimiter;
    private final ConcurrentLinkedDeque<AuditRecord> auditTrail = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<ActionRequestKey> actionReplayOrder = new ConcurrentLinkedDeque<>();
    private final ConcurrentMap<ActionRequestKey, CompletableFuture<ActionReplay>> actionReplays = new ConcurrentHashMap<>();
    private final int maxActionReplays;
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .setVisibility(PropertyAccessor.FIELD,
                    JsonAutoDetect.Visibility.ANY)
            .setVisibility(PropertyAccessor.GETTER,
                    JsonAutoDetect.Visibility.NONE)
            .setVisibility(PropertyAccessor.IS_GETTER,
                    JsonAutoDetect.Visibility.NONE);
    private final HttpServer server;
    private final ExecutorService requestExecutor;

    /**
     * Creates a management agent server.
     *
     * @param bindAddress            host to bind
     * @param bindPort               port to bind (0 means random free port)
     * @param index                  index instance exposed by this agent
     * @param indexName              logical index name returned in responses
     * @throws IOException when HTTP server cannot be created
     */
    public ManagementAgentServer(final String bindAddress, final int bindPort,
            final SegmentIndex<?, ?> index, final String indexName)
            throws IOException {
        this(bindAddress, bindPort,
                ManagementAgentSecurityPolicy.permissive(),
                MAX_ACTION_REPLAYS);
        addIndex(indexName, index);
    }

    /**
     * Creates a management agent server.
     *
     * @param bindAddress            host to bind
     * @param bindPort               port to bind (0 means random free port)
     * @param index                  index instance exposed by this agent
     * @param indexName              logical index name returned in responses
     * @param securityPolicy         authn/authz/rate-limit policy
     * @throws IOException when HTTP server cannot be created
     */
    public ManagementAgentServer(final String bindAddress, final int bindPort,
            final SegmentIndex<?, ?> index, final String indexName,
            final ManagementAgentSecurityPolicy securityPolicy)
            throws IOException {
        this(bindAddress, bindPort, securityPolicy, MAX_ACTION_REPLAYS);
        addIndex(indexName, index);
    }

    /**
     * Creates a management agent server without initial indexes.
     *
     * @param bindAddress            host to bind
     * @param bindPort               port to bind (0 means random free port)
     * @throws IOException when HTTP server cannot be created
     */
    public ManagementAgentServer(final String bindAddress, final int bindPort)
            throws IOException {
        this(bindAddress, bindPort,
                ManagementAgentSecurityPolicy.permissive(),
                MAX_ACTION_REPLAYS);
    }

    /**
     * Creates a management agent server without initial indexes.
     *
     * @param bindAddress            host to bind
     * @param bindPort               port to bind (0 means random free port)
     * @param securityPolicy         authn/authz/rate-limit policy
     * @throws IOException when HTTP server cannot be created
     */
    public ManagementAgentServer(final String bindAddress, final int bindPort,
            final ManagementAgentSecurityPolicy securityPolicy)
            throws IOException {
        this(bindAddress, bindPort, securityPolicy, MAX_ACTION_REPLAYS);
    }

    ManagementAgentServer(final String bindAddress, final int bindPort,
            final ManagementAgentSecurityPolicy securityPolicy,
            final int maxActionReplays)
            throws IOException {
        this.securityPolicy = Objects.requireNonNull(securityPolicy,
                "securityPolicy");
        if (maxActionReplays < 1) {
            throw new IllegalArgumentException(
                    "maxActionReplays must be greater than zero");
        }
        this.maxActionReplays = maxActionReplays;
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
        indexes.put(normalize(indexName, PARAM_INDEX_NAME),
                Objects.requireNonNull(index, "index"));
    }

    /**
     * Removes monitored index registration by name.
     *
     * @param indexName logical index name
     * @return true when entry was removed
     */
    public boolean removeIndex(final String indexName) {
        return indexes.remove(normalize(indexName, PARAM_INDEX_NAME)) != null;
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
    public List<MonitoredIndex> monitoredIndexes() {
        return activeIndexesSnapshot().stream().map(MonitoredIndex.class::cast)
                .toList();
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
        } catch (final RuntimeException e) {
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
                registered -> registered.index().maintenance().flushAndWait());
    }

    private void handleCompact(final HttpExchange exchange) throws IOException {
        handleMutatingAction(exchange, ActionType.COMPACT,
                registered -> registered.index().maintenance().compactAndWait());
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
        final Optional<String> oIndexName = queryParam(exchange, PARAM_INDEX_NAME);
        if (oIndexName.isEmpty()) {
            writeError(exchange, 400, ERROR_INVALID_REQUEST,
                    "Query parameter 'indexName' is required.", "");
            return;
        }
        final RegisteredIndex target = findRegisteredIndex(oIndexName.get())
                .orElse(null);
        if (target == null) {
            writeError(exchange, 404, ERROR_INDEX_NOT_FOUND,
                    UNKNOWN_INDEX_PREFIX + oIndexName.get(), "");
            return;
        }
        final ConfigViewResponse response = toConfigViewResponse(target);
        writeJson(exchange, 200, response);
    }

    private void handleConfigPatch(final HttpExchange exchange,
            final String endpoint) throws IOException {
        final String body = readBodyOrRejectTooLarge(exchange, endpoint);
        if (body == null) {
            return;
        }
        if (!authorize(exchange, AgentRole.ADMIN, true, endpoint, body)) {
            return;
        }
        final ConfigPatchRequest request = parseConfigPatchRequest(exchange,
                endpoint, body);
        if (request == null) {
            return;
        }
        final Optional<RegisteredIndex> target = resolveConfigPatchTarget(
                exchange, endpoint, body);
        if (target.isEmpty()) {
            return;
        }
        final RuntimeTuningPatch runtimePatch = toRuntimePatchOrReject(exchange,
                endpoint, body, request);
        if (runtimePatch == null) {
            return;
        }
        applyConfigPatch(exchange, endpoint, body, request, target.get(),
                runtimePatch);
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
        final MutatingRequestContext context = prepareMutatingRequest(exchange,
                endpoint);
        if (context == null) {
            return;
        }
        final ActionRequestKey replayKey = new ActionRequestKey(
                replayActor(exchange, context.principal()), context.endpoint(),
                context.request().requestId());
        final CompletableFuture<ActionReplay> createdReplay = new CompletableFuture<>();
        final CompletableFuture<ActionReplay> existingReplay = actionReplays
                .putIfAbsent(replayKey, createdReplay);
        if (existingReplay != null) {
            replayAction(exchange, context, existingReplay);
            return;
        }
        boolean keepReplay = false;
        try {
            if (!mutatingRateLimiter.tryAcquire(
                    context.principal().actor() + ":" + context.endpoint())) {
                final ActionReplay replay = new ActionReplay(
                        context.request().indexName(), 429,
                        new ErrorResponse("RATE_LIMITED",
                                "Mutating request rate limit exceeded.",
                                context.request().requestId(), Instant.now()),
                        "REJECTED_RATE_LIMIT");
                createdReplay.complete(replay);
                writeActionReplay(exchange, context.endpoint(), context.body(),
                        replay, replay.auditOutcome());
                return;
            }
            final ActionReplay replay = resolveAndExecuteAction(
                    context.request(), action, operation);
            createdReplay.complete(replay);
            keepReplay = true;
            writeActionReplay(exchange, context.endpoint(), context.body(),
                    replay, replay.auditOutcome());
        } catch (final RuntimeException | IOException e) {
            createdReplay.completeExceptionally(e);
            throw e;
        } finally {
            if (keepReplay) {
                retainReplay(replayKey);
            } else {
                actionReplays.remove(replayKey, createdReplay);
            }
        }
    }

    private MutatingRequestContext prepareMutatingRequest(
            final HttpExchange exchange, final String endpoint)
            throws IOException {
        if (!METHOD_POST.equals(exchange.getRequestMethod())) {
            writeMethodNotAllowed(exchange);
            return null;
        }
        final String body = readBodyOrRejectTooLarge(exchange, endpoint);
        if (body == null) {
            return null;
        }
        final ActionRequest request = parseActionRequest(exchange, endpoint,
                body);
        if (request == null) {
            return null;
        }
        final AuthorizationPrincipal principal = authorizePrincipal(exchange,
                AgentRole.OPERATE, true, endpoint, body, request.requestId(),
                false);
        if (principal == null) {
            return null;
        }
        return new MutatingRequestContext(endpoint, body, request, principal);
    }

    private String readBodyOrRejectTooLarge(final HttpExchange exchange,
            final String endpoint) throws IOException {
        try {
            return readBody(exchange);
        } catch (final RequestBodyTooLargeException e) {
            writeError(exchange, 413, "REQUEST_TOO_LARGE", e.getMessage(), "");
            audit(exchange, endpoint, "", 413, "REJECTED_TOO_LARGE");
            return null;
        }
    }

    private ConfigPatchRequest parseConfigPatchRequest(
            final HttpExchange exchange, final String endpoint,
            final String body) throws IOException {
        try {
            return objectMapper.readValue(body, ConfigPatchRequest.class);
        } catch (final RuntimeException e) {
            final ErrorResponse error = new ErrorResponse(ERROR_INVALID_REQUEST,
                    "Invalid JSON payload.", "", Instant.now());
            writeJson(exchange, 400, error);
            audit(exchange, endpoint, body, 400, AUDIT_FAILED);
            return null;
        }
    }

    private Optional<RegisteredIndex> resolveConfigPatchTarget(
            final HttpExchange exchange, final String endpoint,
            final String body) throws IOException {
        final Optional<String> oIndexName = queryParam(exchange, PARAM_INDEX_NAME);
        if (oIndexName.isEmpty()) {
            writeError(exchange, 400, ERROR_INVALID_REQUEST,
                    "Query parameter 'indexName' is required.", "");
            audit(exchange, endpoint, body, 400, AUDIT_REJECTED);
            return Optional.empty();
        }
        final String indexName = oIndexName.get();
        final RegisteredIndex target = findRegisteredIndex(indexName)
                .orElse(null);
        if (target == null) {
            final ErrorResponse error = new ErrorResponse(ERROR_INDEX_NOT_FOUND,
                    UNKNOWN_INDEX_PREFIX + indexName, "", Instant.now());
            writeJson(exchange, 404, error);
            audit(exchange, endpoint, body, 404, AUDIT_REJECTED);
            return Optional.empty();
        }
        if (!target.ready()) {
            final ErrorResponse error = new ErrorResponse(ERROR_INVALID_STATE,
                    "Index is not in READY state: " + indexName, "",
                    Instant.now());
            writeJson(exchange, 409, error);
            audit(exchange, endpoint, body, 409, AUDIT_REJECTED);
            return Optional.empty();
        }
        return Optional.of(target);
    }

    private RuntimeTuningPatch toRuntimePatchOrReject(
            final HttpExchange exchange, final String endpoint,
            final String body, final ConfigPatchRequest request)
            throws IOException {
        try {
            return toRuntimePatch(request);
        } catch (final ConfigKeyNotSupportedException e) {
            final ErrorResponse error = new ErrorResponse(
                    "CONFIG_KEY_NOT_SUPPORTED", e.getMessage(), "",
                    Instant.now());
            writeJson(exchange, 400, error);
            audit(exchange, endpoint, body, 400, AUDIT_REJECTED);
            return null;
        } catch (final IllegalArgumentException e) {
            final ErrorResponse error = new ErrorResponse(ERROR_INVALID_REQUEST,
                    e.getMessage(), "", Instant.now());
            writeJson(exchange, 400, error);
            audit(exchange, endpoint, body, 400, AUDIT_REJECTED);
            return null;
        }
    }

    private void applyConfigPatch(final HttpExchange exchange,
            final String endpoint, final String body,
            final ConfigPatchRequest request, final RegisteredIndex target,
            final RuntimeTuningPatch runtimePatch) throws IOException {
        if (request.dryRun()) {
            final RuntimeTuningValidation validation = target.index()
                    .runtimeTuning().validate(runtimePatch);
            if (!validation.valid()) {
                writeValidationError(exchange, endpoint, body, validation);
                return;
            }
            writeNoContent(exchange);
            audit(exchange, endpoint, body, 204, "DRY_RUN");
            return;
        }
        final RuntimeTuningResult result = target.index().runtimeTuning()
                .apply(runtimePatch);
        if (!result.validation().valid()) {
            writeValidationError(exchange, endpoint, body,
                    result.validation());
            return;
        }
        if (!result.applied()) {
            final ErrorResponse error = new ErrorResponse(ERROR_INVALID_STATE,
                    "Runtime patch was not applied.", "", Instant.now());
            writeJson(exchange, 409, error);
            audit(exchange, endpoint, body, 409, AUDIT_REJECTED);
            return;
        }
        writeNoContent(exchange);
        audit(exchange, endpoint, body, 204, "APPLIED");
    }

    private void writeValidationError(final HttpExchange exchange,
            final String endpoint, final String body,
            final RuntimeTuningValidation validation) throws IOException {
        final ErrorResponse error = new ErrorResponse(ERROR_INVALID_REQUEST,
                formatValidationIssues(validation), "", Instant.now());
        writeJson(exchange, 400, error);
        audit(exchange, endpoint, body, 400, AUDIT_REJECTED);
    }

    private ActionRequest parseActionRequest(final HttpExchange exchange,
            final String endpoint, final String body) throws IOException {
        try {
            return objectMapper.readValue(body, ActionRequest.class);
        } catch (final RuntimeException e) {
            final ErrorResponse error = new ErrorResponse(ERROR_INVALID_REQUEST,
                    "Invalid JSON payload.", "", Instant.now());
            writeJson(exchange, 400, error);
            audit(exchange, endpoint, body, 400, AUDIT_FAILED);
            return null;
        }
    }

    private ActionReplay resolveAndExecuteAction(final ActionRequest request,
            final ActionType action, final IndexOperation operation) {
        try {
            final List<RegisteredIndex> targets = resolveActionTargets(request);
            if (targets.isEmpty()) {
                return actionErrorReplay(request, 409, ERROR_INVALID_STATE,
                        "No monitored indexes available.", AUDIT_REJECTED);
            }
            final List<String> notReady = notReadyIndexes(targets);
            if (!notReady.isEmpty()) {
                return actionErrorReplay(request, 409, ERROR_INVALID_STATE,
                        "Indexes are not in READY state: "
                                + String.join(",", notReady),
                        AUDIT_REJECTED);
            }
            return executeAction(request, action, operation, targets);
        } catch (final IllegalStateException e) {
            return actionErrorReplay(request, 404, ERROR_INDEX_NOT_FOUND,
                    e.getMessage(), AUDIT_REJECTED);
        }
    }

    private ActionReplay executeAction(final ActionRequest request,
            final ActionType action, final IndexOperation operation,
            final List<RegisteredIndex> targets) {
        int appliedCount = 0;
        String failedIndexName = null;
        try {
            for (final RegisteredIndex target : targets) {
                failedIndexName = target.indexName();
                operation.run(target);
                appliedCount++;
            }
            return new ActionReplay(request.indexName(), 200,
                    new ActionResponse(
                            request.requestId(), action,
                            ActionStatus.COMPLETED,
                            "Applied to " + targets.size() + " index(es).",
                            Instant.now()),
                    "COMPLETED");
        } catch (final RuntimeException e) {
            return new ActionReplay(request.indexName(), 500,
                    new ActionResponse(
                            request.requestId(), action, ActionStatus.FAILED,
                            formatActionFailureMessage(appliedCount,
                                    targets.size(), failedIndexName, e),
                            Instant.now()),
                    appliedCount > 0 ? "FAILED_PARTIAL" : AUDIT_FAILED);
        }
    }

    private ActionReplay actionErrorReplay(final ActionRequest request,
            final int statusCode, final String code, final String message,
            final String auditOutcome) {
        return new ActionReplay(request.indexName(), statusCode,
                new ErrorResponse(code, message, request.requestId(),
                        Instant.now()),
                auditOutcome);
    }

    private void replayAction(final HttpExchange exchange,
            final MutatingRequestContext context,
            final CompletableFuture<ActionReplay> existingReplay)
            throws IOException {
        final ActionReplay replay = awaitReplay(existingReplay);
        if (!Objects.equals(replay.indexName(),
                context.request().indexName())) {
            final ErrorResponse error = new ErrorResponse(
                    "REQUEST_ID_CONFLICT",
                    "requestId is already in use for a different action target.",
                    context.request().requestId(), Instant.now());
            writeJson(exchange, 409, error);
            audit(exchange, context.endpoint(), context.body(), 409,
                    "REJECTED_REQUEST_ID_CONFLICT");
            return;
        }
        writeActionReplay(exchange, context.endpoint(), context.body(), replay,
                "REPLAYED");
    }

    private ActionReplay awaitReplay(
            final CompletableFuture<ActionReplay> existingReplay) {
        try {
            return existingReplay.join();
        } catch (final CompletionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IllegalStateException(
                    "Unable to replay action response.", cause);
        }
    }

    private void writeActionReplay(final HttpExchange exchange,
            final String endpoint, final String body,
            final ActionReplay replay, final String auditOutcome)
            throws IOException {
        writeJson(exchange, replay.statusCode(), replay.payload());
        audit(exchange, endpoint, body, replay.statusCode(), auditOutcome);
    }

    private void retainReplay(final ActionRequestKey replayKey) {
        actionReplayOrder.addLast(replayKey);
        while (actionReplays.size() > maxActionReplays) {
            final ActionRequestKey oldestReplay = actionReplayOrder.pollFirst();
            if (oldestReplay == null) {
                return;
            }
            actionReplays.remove(oldestReplay);
        }
    }

    private String formatActionFailureMessage(final int appliedCount,
            final int totalTargets, final String failedIndexName,
            final RuntimeException error) {
        final String errorMessage = error.getMessage() == null
                || error.getMessage().isBlank()
                        ? error.getClass().getSimpleName()
                        : error.getMessage();
        if (appliedCount <= 0) {
            return "Action failed before applying to any index"
                    + formatFailedIndexSuffix(failedIndexName) + ": "
                    + errorMessage;
        }
        return "Applied to " + appliedCount + " of " + totalTargets
                + " index(es) before failure"
                + formatFailedIndexSuffix(failedIndexName) + ": "
                + errorMessage;
    }

    private String formatFailedIndexSuffix(final String failedIndexName) {
        if (failedIndexName == null || failedIndexName.isBlank()) {
            return "";
        }
        return " on index '" + failedIndexName + "'";
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
                        UNKNOWN_INDEX_PREFIX + request.indexName()));
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
            final SegmentIndexState state = index.runtimeMonitoring().snapshot()
                    .state();
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
                .map(RegisteredIndex::indexName).toList();
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
        final IndexRuntimeSnapshot snapshot = monitored
                .runtimeSnapshot();
        final SegmentIndexState state = snapshot.state();
        return new IndexReportResponse(monitored.indexName(), state.name(),
                state == SegmentIndexState.READY,
                toOperations(snapshot),
                toRegistryCache(snapshot),
                toChunkStoreCache(snapshot),
                toSegments(snapshot),
                toWritePath(snapshot),
                toMaintenance(snapshot),
                toSplit(snapshot),
                toLatency(snapshot),
                toBloomFilter(snapshot),
                toWal(snapshot));
    }

    private OperationReportResponse toOperations(
            final IndexRuntimeSnapshot snapshot) {
        return new OperationReportResponse(
                snapshot.operations().readOperationCount(),
                snapshot.operations().putOperationCount(),
                snapshot.operations().deleteOperationCount());
    }

    private RegistryCacheReportResponse toRegistryCache(
            final IndexRuntimeSnapshot snapshot) {
        return new RegistryCacheReportResponse(
                snapshot.registryCache().hitCount(),
                snapshot.registryCache().missCount(),
                snapshot.registryCache().loadCount(),
                snapshot.registryCache().evictionCount(),
                snapshot.registryCache().size(),
                snapshot.registryCache().limit());
    }

    private ChunkStoreCacheReportResponse toChunkStoreCache(
            final IndexRuntimeSnapshot snapshot) {
        return new ChunkStoreCacheReportResponse(
                snapshot.chunkStoreCache().pageLimit(),
                snapshot.chunkStoreCache().pageCount(),
                snapshot.chunkStoreCache().entryCount(),
                snapshot.chunkStoreCache().hitCount(),
                snapshot.chunkStoreCache().missCount(),
                snapshot.chunkStoreCache().loadCount(),
                snapshot.chunkStoreCache().evictionCount(),
                snapshot.chunkStoreCache().invalidationCount());
    }

    private SegmentReportResponse toSegments(
            final IndexRuntimeSnapshot snapshot) {
        return new SegmentReportResponse(
                snapshot.segments().cacheKeyLimitPerSegment(),
                snapshot.segments().count(),
                snapshot.segments().readyCount(),
                snapshot.segments().maintenanceCount(),
                snapshot.segments().errorCount(),
                snapshot.segments().closedCount(),
                snapshot.segments().unloadedMappedSegmentCount(),
                snapshot.segments().totalKeys(),
                snapshot.segments().totalCacheKeys(),
                snapshot.segments().totalDeltaCacheFiles(),
                toSegmentRuntimeSections(snapshot));
    }

    private WritePathReportResponse toWritePath(
            final IndexRuntimeSnapshot snapshot) {
        return new WritePathReportResponse(
                snapshot.writePath().segmentWriteCacheKeyLimit(),
                snapshot.writePath()
                        .segmentWriteCacheKeyLimitDuringMaintenance(),
                snapshot.writePath().indexBufferedWriteKeyLimit(),
                snapshot.writePath().totalBufferedWriteKeys());
    }

    private MaintenanceReportResponse toMaintenance(
            final IndexRuntimeSnapshot snapshot) {
        return new MaintenanceReportResponse(
                snapshot.maintenance().compactRequestCount(),
                snapshot.maintenance().flushRequestCount(),
                snapshot.maintenance().flushAcceptedToReadyP95Micros(),
                snapshot.maintenance().compactAcceptedToReadyP95Micros(),
                snapshot.maintenance().flushBusyRetryCount(),
                snapshot.maintenance().compactBusyRetryCount(),
                toExecutor(snapshot.maintenance().indexExecutor()),
                toExecutor(snapshot.maintenance().stableSegmentExecutor()));
    }

    private SplitReportResponse toSplit(
            final IndexRuntimeSnapshot snapshot) {
        return new SplitReportResponse(
                snapshot.split().scheduleCount(),
                snapshot.split().inFlightCount(),
                snapshot.split().blockedCount(),
                snapshot.split().taskStartDelayP95Micros(),
                snapshot.split().taskRunLatencyP95Micros(),
                toExecutor(snapshot.split().executor()));
    }

    private ExecutorReportResponse toExecutor(
            final SegmentIndexExecutorMetrics metrics) {
        return new ExecutorReportResponse(metrics.activeThreadCount(),
                metrics.queueSize(), metrics.queueCapacity(),
                metrics.completedTaskCount(), metrics.rejectedTaskCount(),
                metrics.callerRunsCount());
    }

    private LatencyReportResponse toLatency(
            final IndexRuntimeSnapshot snapshot) {
        return new LatencyReportResponse(
                snapshot.latency().readP50Micros(),
                snapshot.latency().readP95Micros(),
                snapshot.latency().readP99Micros(),
                snapshot.latency().writeP50Micros(),
                snapshot.latency().writeP95Micros(),
                snapshot.latency().writeP99Micros());
    }

    private BloomFilterReportResponse toBloomFilter(
            final IndexRuntimeSnapshot snapshot) {
        return new BloomFilterReportResponse(
                snapshot.bloomFilter().hashFunctions(),
                snapshot.bloomFilter().indexSizeInBytes(),
                snapshot.bloomFilter().probabilityOfFalsePositive(),
                snapshot.bloomFilter().requestCount(),
                snapshot.bloomFilter().refusedCount(),
                snapshot.bloomFilter().positiveCount(),
                snapshot.bloomFilter().falsePositiveCount());
    }

    private WalReportResponse toWal(
            final IndexRuntimeSnapshot snapshot) {
        return new WalReportResponse(
                snapshot.wal().enabled(),
                snapshot.wal().appendCount(),
                snapshot.wal().appendBytes(),
                snapshot.wal().syncCount(),
                snapshot.wal().syncFailureCount(),
                snapshot.wal().corruptionCount(),
                snapshot.wal().truncationCount(),
                snapshot.wal().retainedBytes(),
                snapshot.wal().segmentCount(),
                snapshot.wal().durableLsn(),
                snapshot.wal().checkpointLsn(),
                snapshot.wal().pendingSyncBytes(),
                snapshot.wal().appliedLsn(),
                snapshot.wal().checkpointLagLsn(),
                snapshot.wal().syncTotalNanos(),
                snapshot.wal().syncMaxNanos(),
                snapshot.wal().syncBatchBytesTotal(),
                snapshot.wal().syncBatchBytesMax(),
                snapshot.wal().syncAverageNanos(),
                snapshot.wal().syncAverageBatchBytes());
    }

    private List<SegmentRuntimeReportResponse> toSegmentRuntimeSections(
            final IndexRuntimeSnapshot snapshot) {
        return snapshot.segments().runtimeMetrics().stream()
                .map(this::toSegmentRuntimeSection).toList();
    }

    private SegmentRuntimeReportResponse toSegmentRuntimeSection(
            final SegmentIndexSegmentRuntimeMetrics segment) {
        return new SegmentRuntimeReportResponse(segment.segmentId(),
                segment.state().name(),
                segment.numberOfKeysInDeltaCache(),
                segment.numberOfKeysInSegment(),
                segment.numberOfKeysInScarceIndex(),
                segment.numberOfKeysInSegmentCache(),
                segment.numberOfKeysInWriteCache(),
                segment.numberOfDeltaCacheFiles(),
                segment.compactRequestCount(),
                segment.flushRequestCount(),
                segment.bloomFilterRequestCount(),
                segment.bloomFilterRefusedCount(),
                segment.bloomFilterPositiveCount(),
                segment.bloomFilterFalsePositiveCount());
    }

    private ConfigViewResponse toConfigViewResponse(
            final RegisteredIndex monitored) {
        final RuntimeTuningSnapshot currentConfig = monitored.index()
                .runtimeTuning().current();
        final RuntimeTuningSnapshot originalConfig = monitored.index()
                .runtimeTuning().original();
        return new ConfigViewResponse(monitored.indexName(),
                toApiConfigMap(originalConfig),
                toApiConfigMap(currentConfig),
                supportedRuntimeConfigKeys(),
                currentConfig.revision(), Instant.now());
    }

    private RuntimeTuningPatch toRuntimePatch(
            final ConfigPatchRequest request) {
        final RuntimeTuningPatchBuilder builder =
                RuntimeTuningPatch.builder();
        for (final Map.Entry<String, String> entry : request.values()
                .entrySet()) {
            final String key = entry.getKey();
            final RuntimeTuningField runtimeField = RUNTIME_FIELD_BY_API_NAME
                    .get(key);
            if (runtimeField == null) {
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
            applyRuntimePatchValue(builder, runtimeField, value);
        }
        return builder.build();
    }

    private static List<String> supportedRuntimeConfigKeys() {
        return SUPPORTED_RUNTIME_CONFIG_KEYS;
    }

    private static void applyRuntimePatchValue(
            final RuntimeTuningPatchBuilder builder,
            final RuntimeTuningField field, final int value) {
        switch (field) {
            case SEGMENT_CACHED_SEGMENT_LIMIT -> builder
                    .segment(segment -> segment.cachedSegmentLimit(value));
            case SEGMENT_CACHE_KEY_LIMIT -> builder
                    .segment(segment -> segment.cacheKeyLimit(value));
            case WRITE_PATH_SEGMENT_WRITE_CACHE_KEY_LIMIT -> builder
                    .writePath(writePath -> writePath
                            .segmentWriteCacheKeyLimit(value));
            case WRITE_PATH_SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE -> builder
                    .writePath(writePath -> writePath
                            .segmentWriteCacheKeyLimitDuringMaintenance(
                                    value));
            case WRITE_PATH_INDEX_BUFFERED_WRITE_KEY_LIMIT -> builder
                    .writePath(writePath -> writePath
                            .indexBufferedWriteKeyLimit(value));
            case WRITE_PATH_SEGMENT_SPLIT_KEY_THRESHOLD -> builder
                    .writePath(writePath -> writePath
                            .segmentSplitKeyThreshold(value));
            case CHUNK_STORE_CACHE_PAGE_LIMIT -> builder
                    .chunkStoreCache(cache -> cache.pageLimit(value));
        }
    }

    private String formatValidationIssues(
            final RuntimeTuningValidation validation) {
        return validation.issues().stream().map(issue -> {
            if (issue.field() == null) {
                return issue.message();
            }
            return issue.field().path() + ": " + issue.message();
        }).collect(Collectors.joining("; "));
    }

    private Optional<RegisteredIndex> findRegisteredIndex(
            final String indexName) {
        final String normalizedName = normalize(indexName, PARAM_INDEX_NAME);
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
            if (queryPart != null && !queryPart.isEmpty()) {
                final int equalsIndex = queryPart.indexOf('=');
                final String rawKey = equalsIndex >= 0
                        ? queryPart.substring(0, equalsIndex)
                        : queryPart;
                final String decodedKey = URLDecoder.decode(rawKey,
                        StandardCharsets.UTF_8);
                if (key.equals(decodedKey)) {
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
            }
        }
        return Optional.empty();
    }

    private static Map<String, Integer> toApiConfigMap(
            final RuntimeTuningSnapshot runtimeValues) {
        final LinkedHashMap<String, Integer> values = new LinkedHashMap<>();
        for (final Map.Entry<RuntimeTuningField, String> entry : API_NAME_BY_RUNTIME_FIELD
                .entrySet()) {
            values.put(entry.getValue(),
                    Integer.valueOf(value(runtimeValues, entry.getKey())));
        }
        return Map.copyOf(values);
    }

    private static int value(final RuntimeTuningSnapshot snapshot,
            final RuntimeTuningField field) {
        return switch (field) {
            case SEGMENT_CACHED_SEGMENT_LIMIT -> snapshot.segment()
                    .cachedSegmentLimit();
            case SEGMENT_CACHE_KEY_LIMIT -> snapshot.segment()
                    .cacheKeyLimit();
            case WRITE_PATH_SEGMENT_WRITE_CACHE_KEY_LIMIT -> snapshot
                    .writePath().segmentWriteCacheKeyLimit();
            case WRITE_PATH_SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE -> snapshot
                    .writePath()
                    .segmentWriteCacheKeyLimitDuringMaintenance();
            case WRITE_PATH_INDEX_BUFFERED_WRITE_KEY_LIMIT -> snapshot
                    .writePath().indexBufferedWriteKeyLimit();
            case WRITE_PATH_SEGMENT_SPLIT_KEY_THRESHOLD -> snapshot.writePath()
                    .segmentSplitKeyThreshold();
            case CHUNK_STORE_CACHE_PAGE_LIMIT -> snapshot.chunkStoreCache()
                    .pageLimit();
        };
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
        return authorizePrincipal(exchange, requiredRole, mutating, endpoint,
                requestBody, "", true) != null;
    }

    private AuthorizationPrincipal authorizePrincipal(
            final HttpExchange exchange, final AgentRole requiredRole,
            final boolean mutating, final String endpoint,
            final String requestBody, final String requestId,
            final boolean applyRateLimit)
            throws IOException {
        if (securityPolicy.requireTls() && !isSecureTransport(exchange)) {
            rejectAuthorization(exchange, mutating, endpoint, requestBody, 400,
                    "TLS_REQUIRED", "HTTPS transport is required.",
                    "REJECTED_TLS", requestId);
            return null;
        }
        final AuthorizationPrincipal principal = resolveAuthorizationPrincipal(
                exchange, mutating, endpoint, requestBody, requestId);
        if (principal == null) {
            return null;
        }
        if (!principal.role().allows(requiredRole)) {
            rejectAuthorization(exchange, mutating, endpoint, requestBody, 403,
                    "FORBIDDEN", "Insufficient privileges for endpoint.",
                    "REJECTED_FORBIDDEN", requestId);
            return null;
        }
        if (applyRateLimit && mutating && !mutatingRateLimiter
                .tryAcquire(principal.actor() + ":" + endpoint)) {
            rejectAuthorization(exchange, true, endpoint, requestBody, 429,
                    "RATE_LIMITED", "Mutating request rate limit exceeded.",
                    "REJECTED_RATE_LIMIT", requestId);
            return null;
        }
        return principal;
    }

    private AuthorizationPrincipal resolveAuthorizationPrincipal(
            final HttpExchange exchange, final boolean mutating,
            final String endpoint, final String requestBody,
            final String requestId)
            throws IOException {
        if (securityPolicy.tokenRoles().isEmpty()) {
            return new AuthorizationPrincipal(AgentRole.ADMIN, "anonymous");
        }
        final String token = readToken(exchange);
        if (token == null) {
            rejectAuthorization(exchange, mutating, endpoint, requestBody, 401,
                    "UNAUTHORIZED", "Authentication token is required.",
                    "REJECTED_UNAUTHORIZED", requestId);
            return null;
        }
        final AgentRole actualRole = securityPolicy.tokenRoles().get(token);
        if (actualRole == null) {
            rejectAuthorization(exchange, mutating, endpoint, requestBody, 401,
                    "UNAUTHORIZED", "Authentication token is invalid.",
                    "REJECTED_UNAUTHORIZED", requestId);
            return null;
        }
        return new AuthorizationPrincipal(actualRole, token);
    }

    private boolean rejectAuthorization(final HttpExchange exchange,
            final boolean mutating, final String endpoint,
            final String requestBody, final int statusCode, final String code,
            final String message, final String auditOutcome,
            final String requestId)
            throws IOException {
        writeError(exchange, statusCode, code, message, requestId);
        if (mutating) {
            audit(exchange, endpoint, requestBody, statusCode, auditOutcome);
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

    private static Map<String, RuntimeTuningField> buildRuntimeFieldByApiName() {
        final LinkedHashMap<String, RuntimeTuningField> values =
                new LinkedHashMap<>();
        for (final Map.Entry<RuntimeTuningField, String> entry : API_NAME_BY_RUNTIME_FIELD
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

    private String replayActor(final HttpExchange exchange,
            final AuthorizationPrincipal principal) {
        if (!"anonymous".equals(principal.actor())) {
            return principal.actor();
        }
        if (exchange.getRemoteAddress() == null) {
            return "anonymous";
        }
        return "anonymous@" + exchange.getRemoteAddress().getHostString();
    }

    @FunctionalInterface
    private interface Handler {
        void handle(HttpExchange exchange) throws IOException;
    }

    @FunctionalInterface
    private interface IndexOperation {
        void run(RegisteredIndex index);
    }

    private record MutatingRequestContext(String endpoint, String body,
            ActionRequest request, AuthorizationPrincipal principal) {
    }

    private record AuthorizationPrincipal(AgentRole role, String actor) {
    }

    private record ActionRequestKey(String actor, String endpoint,
            String requestId) {
    }

    private record ActionReplay(String indexName, int statusCode, Object payload,
            String auditOutcome) {
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
            return index.runtimeMonitoring().snapshot().state();
        }

        @Override
        public IndexRuntimeSnapshot runtimeSnapshot() {
            return index.runtimeMonitoring().snapshot();
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
