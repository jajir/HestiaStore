package org.hestiastore.index.segment;

/**
 * Inputs required to execute a split pipeline once.
 * Now mutable with getters/setters to ease validation and future extensions.
 */
final class SegmentSplitContext<K, V> {
    private SegmentImpl<K, V> segment;
    private VersionController versionController;
    private SegmentSplitterPlan<K, V> plan;
    private SegmentId lowerSegmentId;

    SegmentSplitContext(final SegmentImpl<K, V> segment,
            final VersionController versionController,
            final SegmentSplitterPlan<K, V> plan,
            final SegmentId lowerSegmentId) {
        this.segment = segment;
        this.versionController = versionController;
        this.plan = plan;
        this.lowerSegmentId = lowerSegmentId;
    }

    SegmentImpl<K, V> getSegment() {
        return segment;
    }

    void setSegment(final SegmentImpl<K, V> segment) {
        this.segment = segment;
    }

    VersionController getVersionController() {
        return versionController;
    }

    void setVersionController(final VersionController versionController) {
        this.versionController = versionController;
    }

    SegmentSplitterPlan<K, V> getPlan() {
        return plan;
    }

    void setPlan(final SegmentSplitterPlan<K, V> plan) {
        this.plan = plan;
    }

    SegmentId getLowerSegmentId() {
        return lowerSegmentId;
    }

    void setLowerSegmentId(final SegmentId lowerSegmentId) {
        this.lowerSegmentId = lowerSegmentId;
    }
}
