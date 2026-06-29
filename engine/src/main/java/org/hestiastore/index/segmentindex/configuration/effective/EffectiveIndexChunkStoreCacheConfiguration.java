package org.hestiastore.index.segmentindex.configuration.effective;

import org.hestiastore.index.Vldtn;

/**
 * Resolved parsed persisted chunk page cache configuration.
 */
public final class EffectiveIndexChunkStoreCacheConfiguration {

    private final int pageLimit;

    public EffectiveIndexChunkStoreCacheConfiguration(final int pageLimit) {
        this.pageLimit = Vldtn.requireGreaterThanOrEqualToZero(pageLimit,
                "pageLimit");
    }

    public int pageLimit() {
        return pageLimit;
    }
}
