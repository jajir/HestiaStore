package org.hestiastore.index.segmentindex.configuration.tuning;

import org.hestiastore.index.Vldtn;

/**
 * Typed runtime parsed chunk page cache tuning snapshot.
 */
public final class RuntimeChunkStoreCacheTuningSnapshot {

    private final int pageLimit;

    RuntimeChunkStoreCacheTuningSnapshot(final int pageLimit) {
        this.pageLimit = Vldtn.requireGreaterThanOrEqualToZero(pageLimit,
                "pageLimit");
    }

    public int pageLimit() {
        return pageLimit;
    }
}
