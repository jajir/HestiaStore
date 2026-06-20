package org.hestiastore.index.segmentindex.core.session;

/**
 * Selects the lifecycle operation handled by one bootstrap run.
 */
enum SegmentIndexBootstrapMode {

    /**
     * Create a new index and persist its effective configuration.
     */
    CREATE,

    /**
     * Open an existing index and fail when stored configuration is absent.
     */
    OPEN,

    /**
     * Open an existing index only when stored configuration is present.
     */
    TRY_OPEN
}
