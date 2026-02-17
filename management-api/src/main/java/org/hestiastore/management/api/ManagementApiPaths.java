package org.hestiastore.management.api;

/**
 * Management API endpoint paths (v1).
 */
public final class ManagementApiPaths {

    /**
     * Current API version segment.
     */
    public static final String VERSION = "v1";

    /**
     * API version prefix.
     */
    public static final String BASE = "/api/" + VERSION;

    /**
     * Endpoint exposing node lifecycle state.
     */
    public static final String STATE = BASE + "/state";

    /**
     * Endpoint exposing metric snapshot.
     */
    public static final String METRICS = BASE + "/metrics";

    /**
     * Endpoint for flush action requests.
     */
    public static final String ACTION_FLUSH = BASE + "/actions/flush";

    /**
     * Endpoint for compact action requests.
     */
    public static final String ACTION_COMPACT = BASE + "/actions/compact";

    /**
     * Endpoint for runtime-safe config patch requests.
     */
    public static final String CONFIG = BASE + "/config";

    private ManagementApiPaths() {
    }
}
