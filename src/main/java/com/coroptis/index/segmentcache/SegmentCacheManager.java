package com.coroptis.index.segmentcache;

import java.util.Objects;

import com.coroptis.index.PairWriter;
import com.coroptis.index.segment.SegmentFiles;
import com.coroptis.index.segment.SegmentId;
import com.coroptis.index.segment.SegmentStatsManager;
import com.coroptis.index.sst.SegmentManager;

/**
 * 
 * TODO adding data to segment should be creating of one file.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentCacheManager<K, V> {

    private SegmentManager<K, V> segmentManager;

    private SegmentId segmentId;

    private SegmentFiles<K, V> segmentFiles;

    private SegmentStatsManager segmentStatsManager;

    public SegmentCacheManager(final SegmentManager<K, V> segmentManager,
            SegmentFiles<K, V> segmentFiles,
            SegmentStatsManager segmentStatsManager) {
        this.segmentManager = Objects.requireNonNull(segmentManager);
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.segmentStatsManager = Objects.requireNonNull(segmentStatsManager);
        this.segmentId = segmentFiles.getId();
    }

    public PairWriter<K, V> openWriter() {
        SegmentCacheWriter<K, V> writer;
        if (segmentManager.isInCache(segmentId)) {
            writer = new SegmentCacheWriterCached<>(segmentFiles,
                    segmentManager.getSegment(segmentId).getCache(),
                    segmentStatsManager);
        } else {
            writer = new SegmentCacheWriterPlain<>(segmentFiles,
                    segmentFiles.getKeyTypeDescriptor(), segmentStatsManager);
        }
        return writer.openWriter();
    }
}
