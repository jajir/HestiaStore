package org.hestiastore.index.segmentindex.core.bootstrap;

/**
 * Outcome of one segment-index bootstrap run.
 */
enum SegmentIndexBootstrapStatus {

    /**
     * A new index was created.
     */
    CREATED,

    /**
     * An existing index was opened.
     */
    OPENED,

    /**
     * A try-open request found no stored index configuration.
     */
    NOT_FOUND
}
