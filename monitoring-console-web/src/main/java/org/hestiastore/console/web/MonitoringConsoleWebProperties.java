package org.hestiastore.console.web;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Web console configuration properties.
 *
 * @param backendBaseUrl base URL of monitoring-console backend
 * @param writeToken     optional write token for mutating backend operations
 * @param refreshMillis  auto refresh interval in milliseconds
 */
@ConfigurationProperties(prefix = "hestia.console.web")
public record MonitoringConsoleWebProperties(String backendBaseUrl,
        String writeToken, long refreshMillis) {

    /**
     * Validation constructor.
     */
    public MonitoringConsoleWebProperties {
        backendBaseUrl = normalize(backendBaseUrl,
                "backendBaseUrl");
        writeToken = writeToken == null ? "" : writeToken.trim();
        if (refreshMillis < 1000L) {
            throw new IllegalArgumentException(
                    "refreshMillis must be >= 1000");
        }
    }

    private static String normalize(final String value, final String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim().endsWith("/")
                ? value.trim().substring(0, value.trim().length() - 1)
                : value.trim();
    }
}
