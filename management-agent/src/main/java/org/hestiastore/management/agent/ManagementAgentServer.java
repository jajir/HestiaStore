package org.hestiastore.management.agent;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Set;

import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.management.api.ActionRequest;
import org.hestiastore.management.api.ActionResponse;
import org.hestiastore.management.api.ActionStatus;
import org.hestiastore.management.api.ActionType;
import org.hestiastore.management.api.ConfigPatchRequest;
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
 * Lightweight node-local HTTP management agent.
 */
public final class ManagementAgentServer implements AutoCloseable {

    private static final String METHOD_GET = "GET";
    private static final String METHOD_POST = "POST";
    private static final String METHOD_PATCH = "PATCH";

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final SegmentIndex<?, ?> index;
    private final String indexName;
    private final Set<String> runtimeConfigAllowlist;
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());
    private final HttpServer server;

    /**
     * Creates a management agent server.
     *
     * @param bindAddress            host to bind
     * @param bindPort               port to bind (0 means random free port)
     * @param index                  index instance exposed by this agent
     * @param indexName              logical index name returned in responses
     * @param runtimeConfigAllowlist keys accepted by PATCH /config
     * @throws IOException when HTTP server cannot be created
     */
    public ManagementAgentServer(final String bindAddress, final int bindPort,
            final SegmentIndex<?, ?> index, final String indexName,
            final Set<String> runtimeConfigAllowlist) throws IOException {
        this.index = Objects.requireNonNull(index, "index");
        this.indexName = Objects.requireNonNull(indexName, "indexName").trim();
        if (this.indexName.isEmpty()) {
            throw new IllegalArgumentException("indexName must not be blank");
        }
        this.runtimeConfigAllowlist = Set.copyOf(
                Objects.requireNonNull(runtimeConfigAllowlist,
                        "runtimeConfigAllowlist"));
        this.server = HttpServer.create(
                new InetSocketAddress(
                        Objects.requireNonNull(bindAddress, "bindAddress"),
                        bindPort),
                0);
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

    /** {@inheritDoc} */
    @Override
    public void close() {
        server.stop(0);
    }

    private void registerRoutes() {
        server.createContext(ManagementApiPaths.STATE,
                exchange -> safeHandle(exchange, this::handleState));
        server.createContext(ManagementApiPaths.METRICS,
                exchange -> safeHandle(exchange, this::handleMetrics));
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

    private void safeHandle(final HttpExchange exchange,
            final Handler handler) throws IOException {
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

    private void handleState(final HttpExchange exchange) throws IOException {
        if (!METHOD_GET.equals(exchange.getRequestMethod())) {
            writeMethodNotAllowed(exchange);
            return;
        }
        final SegmentIndexState state = index.getState();
        final NodeStateResponse response = new NodeStateResponse(indexName,
                state.name(), state == SegmentIndexState.READY, Instant.now());
        writeJson(exchange, 200, response);
    }

    private void handleMetrics(final HttpExchange exchange) throws IOException {
        if (!METHOD_GET.equals(exchange.getRequestMethod())) {
            writeMethodNotAllowed(exchange);
            return;
        }
        final SegmentIndexMetricsSnapshot snapshot = index.metricsSnapshot();
        final MetricsResponse response = new MetricsResponse(indexName,
                snapshot.getState().name(), snapshot.getGetOperationCount(),
                snapshot.getPutOperationCount(),
                snapshot.getDeleteOperationCount(), Instant.now());
        writeJson(exchange, 200, response);
    }

    private void handleFlush(final HttpExchange exchange) throws IOException {
        handleMutatingAction(exchange, ActionType.FLUSH,
                () -> index.flushAndWait());
    }

    private void handleCompact(final HttpExchange exchange) throws IOException {
        handleMutatingAction(exchange, ActionType.COMPACT,
                () -> index.compactAndWait());
    }

    private void handleConfig(final HttpExchange exchange) throws IOException {
        final String endpoint = ManagementApiPaths.CONFIG;
        final String body = readBody(exchange);
        if (!METHOD_PATCH.equals(exchange.getRequestMethod())) {
            writeMethodNotAllowed(exchange);
            return;
        }
        if (!isReady()) {
            final ErrorResponse error = new ErrorResponse("INVALID_STATE",
                    "Index is not in READY state.", "", Instant.now());
            writeJson(exchange, 409, error);
            audit(exchange, endpoint, body, 409, "REJECTED");
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
        for (final String key : request.values().keySet()) {
            if (!runtimeConfigAllowlist.contains(key)) {
                final ErrorResponse error = new ErrorResponse(
                        "CONFIG_KEY_NOT_ALLOWED",
                        "Config key '" + key + "' is not allowed.", "",
                        Instant.now());
                writeJson(exchange, 400, error);
                audit(exchange, endpoint, body, 400, "REJECTED");
                return;
            }
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
        writeJson(exchange, 200, "{\"status\":\"UP\"}");
    }

    private void handleReady(final HttpExchange exchange) throws IOException {
        if (!METHOD_GET.equals(exchange.getRequestMethod())) {
            writeMethodNotAllowed(exchange);
            return;
        }
        if (isReady()) {
            writeJson(exchange, 200, "{\"status\":\"READY\"}");
        } else {
            writeJson(exchange, 503, "{\"status\":\"NOT_READY\"}");
        }
    }

    private void handleMutatingAction(final HttpExchange exchange,
            final ActionType action, final Runnable operation)
            throws IOException {
        final String endpoint = exchange.getRequestURI().getPath();
        final String body = readBody(exchange);
        if (!METHOD_POST.equals(exchange.getRequestMethod())) {
            writeMethodNotAllowed(exchange);
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
        if (!isReady()) {
            final ErrorResponse error = new ErrorResponse("INVALID_STATE",
                    "Index is not in READY state.", request.requestId(),
                    Instant.now());
            writeJson(exchange, 409, error);
            audit(exchange, endpoint, body, 409, "REJECTED");
            return;
        }
        try {
            operation.run();
            final ActionResponse response = new ActionResponse(
                    request.requestId(), action, ActionStatus.COMPLETED, "",
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

    private boolean isReady() {
        return index.getState() == SegmentIndexState.READY;
    }

    private void writeMethodNotAllowed(final HttpExchange exchange)
            throws IOException {
        final ErrorResponse error = new ErrorResponse("METHOD_NOT_ALLOWED",
                "HTTP method is not allowed.", "", Instant.now());
        writeJson(exchange, 405, error);
    }

    private String readBody(final HttpExchange exchange) throws IOException {
        final byte[] bytes = exchange.getRequestBody().readAllBytes();
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private void writeNoContent(final HttpExchange exchange) throws IOException {
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
        logger.info(
                "audit endpoint={} actor={} status={} outcome={} digest={} timestamp={}",
                endpoint, actor, statusCode, outcome, digest, Instant.now());
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

    @FunctionalInterface
    private interface Handler {
        void handle(HttpExchange exchange) throws Exception;
    }
}
