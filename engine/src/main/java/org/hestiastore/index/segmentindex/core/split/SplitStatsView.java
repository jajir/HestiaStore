package org.hestiastore.index.segmentindex.core.split;

/**
 * Stats-facing split runtime view.
 */
public interface SplitStatsView {

    /**
     * Returns the current immutable split runtime stats snapshot.
     *
     * @return split runtime stats snapshot
     */
    SplitStats statsSnapshot();
}
