package com.coroptis.index.segmentcache;

import java.util.Objects;

import com.coroptis.index.segment.Segment;
import com.coroptis.index.segment.SegmentId;
import com.coroptis.index.segment.SegmentStatsManager;
import com.coroptis.index.sst.SegmentManager;

public class SegmentCompacter<K, V> {

    private SegmentStatsManager segmentStatsManager;

    private long maxNumberOfKeysInSegmentCache;

    private SegmentManager<K, V> segmentManager;

    private SegmentId segmentId;

    public SegmentCompacter(SegmentStatsManager segmentStatsManager,
            long maxNumberOfKeysInSegmentCache,
            SegmentManager<K, V> segmentManager, SegmentId segmentId) {
        this.segmentStatsManager = Objects.requireNonNull(segmentStatsManager);
        this.maxNumberOfKeysInSegmentCache = Objects
                .requireNonNull(maxNumberOfKeysInSegmentCache);
        this.segmentManager = Objects.requireNonNull(segmentManager);
        this.segmentId = Objects.requireNonNull(segmentId);
    }

    /**
     * It's called after writing is done. It ensure that all data are stored in
     * directory.
     */
    public void flush() {
        if (segmentStatsManager.getSegmentStats()
                .getNumberOfKeysInCache() > maxNumberOfKeysInSegmentCache) {
            final Segment<K, V> segment = segmentManager.getSegment(segmentId);
            segment.flush();
        }
    }

}
