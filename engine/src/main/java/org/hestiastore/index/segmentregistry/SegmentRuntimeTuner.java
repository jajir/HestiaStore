package org.hestiastore.index.segmentregistry;

import org.hestiastore.index.segment.SegmentRuntimeLimits;

/**
 * Applies runtime-only tuning used for newly materialized segments.
 */
interface SegmentRuntimeTuner {

    /**
     * Updates runtime-only limits used for future segment materialization.
     *
     * @param runtimeLimits validated segment runtime limits
     */
    void updateRuntimeLimits(SegmentRuntimeLimits runtimeLimits);
}
