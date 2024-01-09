package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.OptimisticLock;
import com.coroptis.index.OptimisticLockObjectVersionProvider;

/**
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentSearcherManager<K, V> implements CloseableResource {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentConf segmentConf;
    private final OptimisticLockObjectVersionProvider versionProvider;

    private OptimisticLock lock;
    private SegmentSearcher<K, V> segmentSearcher;

    public SegmentSearcherManager(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final OptimisticLockObjectVersionProvider versionProvider) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.segmentConf = Objects.requireNonNull(segmentConf);
        this.versionProvider = Objects.requireNonNull(versionProvider);
        lock = new OptimisticLock(versionProvider);
    }

    public SegmentSearcher<K, V> getSearcher() {
        if (lock.isLocked()) {
            segmentSearcher = null;
        }
        if (segmentSearcher == null) {
            segmentSearcher = makeSearcher();
            lock = new OptimisticLock(versionProvider);
        }
        return segmentSearcher;
    }

    private SegmentSearcher<K, V> makeSearcher() {
        return new SegmentSearcher<>(segmentFiles, segmentConf);
    }

    @Override
    public void close() {
        if (segmentSearcher == null) {
            return;
        }
        segmentSearcher.close();
        segmentSearcher = null;
    }

}
