package org.hestiastore.console.web;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Web console configuration properties.
 *
 * @param backendBaseUrl legacy single-node base URL (deprecated)
 * @param nodes          managed node endpoints (direct monitoring-rest-json mode)
 * @param refreshMillis  auto refresh interval in milliseconds
 */
@ConfigurationProperties(prefix = "hestia.console.web")
public record MonitoringConsoleWebProperties(String backendBaseUrl,
        List<NodeEndpoint> nodes, long refreshMillis) {

    /**
     * Validation constructor.
     */
    public MonitoringConsoleWebProperties {
        backendBaseUrl = normalizeOptional(backendBaseUrl);
        final List<NodeEndpoint> resolvedNodes = new ArrayList<>();
        if (nodes != null) {
            for (final NodeEndpoint node : nodes) {
                resolvedNodes.add(node);
            }
        }
        if (resolvedNodes.isEmpty()) {
            if (backendBaseUrl.isEmpty()) {
                throw new IllegalArgumentException(
                        "nodes must not be empty");
            }
            resolvedNodes.add(new NodeEndpoint("node-1", "node-1",
                    backendBaseUrl, ""));
        }
        nodes = List.copyOf(resolvedNodes);
        if (refreshMillis < 1000L) {
            throw new IllegalArgumentException(
                    "refreshMillis must be >= 1000");
        }
    }

    private static String normalizeOptional(final String value) {
        if (value == null || value.trim().isEmpty()) {
            return "";
        }
        return value.trim().endsWith("/")
                ? value.trim().substring(0, value.trim().length() - 1)
                : value.trim();
    }

    /**
     * One managed node configuration.
     *
     * @param nodeId    unique node identifier
     * @param nodeName  display name used by UI
     * @param baseUrl   monitoring-rest-json base URL
     * @param agentToken optional bearer token sent to monitoring-rest-json
     */
    public record NodeEndpoint(String nodeId, String nodeName, String baseUrl,
            String agentToken) {
        /**
         * Validation constructor.
         */
        public NodeEndpoint {
            nodeId = normalizeRequired(nodeId, "nodeId");
            nodeName = normalizeRequired(nodeName, "nodeName");
            baseUrl = normalizeRequired(baseUrl, "baseUrl");
            if (!baseUrl.startsWith("http://")
                    && !baseUrl.startsWith("https://")) {
                throw new IllegalArgumentException(
                        "baseUrl must use http(s)");
            }
            if (baseUrl.endsWith("/")) {
                baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
            }
            agentToken = agentToken == null ? "" : agentToken.trim();
        }

        private static String normalizeRequired(final String value,
                final String name) {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        name + " must not be blank");
            }
            return value.trim();
        }
    }
}
