package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;

/**
 * Immutable snapshot of index-level operation counters.
 */
public final class SegmentIndexMetricsSnapshot {

    private final long getOperationCount;
    private final long putOperationCount;
    private final long deleteOperationCount;
    private final SegmentIndexState state;

    /**
     * Creates a new immutable metrics snapshot.
     *
     * @param getOperationCount    number of get calls
     * @param putOperationCount    number of put calls
     * @param deleteOperationCount number of delete calls
     * @param state                current index lifecycle state
     */
    public SegmentIndexMetricsSnapshot(final long getOperationCount,
            final long putOperationCount, final long deleteOperationCount,
            final SegmentIndexState state) {
        if (getOperationCount < 0) {
            throw new IllegalArgumentException(
                    "getOperationCount must be >= 0");
        }
        if (putOperationCount < 0) {
            throw new IllegalArgumentException(
                    "putOperationCount must be >= 0");
        }
        if (deleteOperationCount < 0) {
            throw new IllegalArgumentException(
                    "deleteOperationCount must be >= 0");
        }
        this.getOperationCount = getOperationCount;
        this.putOperationCount = putOperationCount;
        this.deleteOperationCount = deleteOperationCount;
        this.state = Vldtn.requireNonNull(state, "state");
    }

    /**
     * Returns the number of get operations recorded by the index.
     *
     * @return get operation count
     */
    public long getGetOperationCount() {
        return getOperationCount;
    }

    /**
     * Returns the number of put operations recorded by the index.
     *
     * @return put operation count
     */
    public long getPutOperationCount() {
        return putOperationCount;
    }

    /**
     * Returns the number of delete operations recorded by the index.
     *
     * @return delete operation count
     */
    public long getDeleteOperationCount() {
        return deleteOperationCount;
    }

    /**
     * Returns the state captured with this snapshot.
     *
     * @return index state at snapshot time
     */
    public SegmentIndexState getState() {
        return state;
    }

}
