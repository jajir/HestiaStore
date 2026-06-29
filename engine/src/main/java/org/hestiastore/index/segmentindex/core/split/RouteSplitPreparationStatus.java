package org.hestiastore.index.segmentindex.core.split;

/**
 * Outcome of split preparation before route-map publication.
 */
enum RouteSplitPreparationStatus {

    /**
     * Child segments were materialized and can be published.
     */
    PREPARED,

    /**
     * Split preparation was skipped without parent maintenance.
     */
    SKIPPED,

    /**
     * Parent should be compacted before another split attempt.
     */
    COMPACT_PARENT
}
