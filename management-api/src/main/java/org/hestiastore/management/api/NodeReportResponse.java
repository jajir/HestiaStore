package org.hestiastore.management.api;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Node report payload with common JVM section and per-index sections.
 *
 * @param jvm        node-wide JVM metrics
 * @param indexes    per-index metrics sections
 * @param capturedAt report timestamp
 */
public record NodeReportResponse(JvmMetricsResponse jvm,
        List<IndexReportResponse> indexes, Instant capturedAt) {

    /**
     * Creates validated node report payload.
     */
    public NodeReportResponse {
        jvm = Objects.requireNonNull(jvm, "jvm");
        indexes = List.copyOf(Objects.requireNonNull(indexes, "indexes"));
        capturedAt = Objects.requireNonNull(capturedAt, "capturedAt");
    }
}
