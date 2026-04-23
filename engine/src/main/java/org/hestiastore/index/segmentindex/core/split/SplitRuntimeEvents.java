package org.hestiastore.index.segmentindex.core.split;

/**
 * Internal split-runtime events exchanged between split execution and policy
 * scheduling components.
 */
interface SplitRuntimeEvents {

    /**
     * Signals that a split was successfully applied to the route topology.
     */
    void onSplitApplied();

    /**
     * @return no-op split event sink
     */
    static SplitRuntimeEvents noOp() {
        return new SplitRuntimeEvents() {
            @Override
            public void onSplitApplied() {
            }
        };
    }
}
