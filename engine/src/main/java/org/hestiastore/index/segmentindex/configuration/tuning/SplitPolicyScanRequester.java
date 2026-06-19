package org.hestiastore.index.segmentindex.configuration.tuning;

/**
 * Requests split-policy work after runtime tuning changes affect split
 * thresholds.
 */
public interface SplitPolicyScanRequester {

    /**
     * Requests a full split scan using the latest runtime tuning values.
     */
    void requestFullSplitScan();
}
