package com.coroptis.index.segment;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.OptimisticLock;
import com.coroptis.index.OptimisticLockObjectVersionProvider;

/**
 * Object use in memory cache and bloom filter. Only one instance for one
 * segment should be in memory at the time.
 * 
 * This object can be cached in memory.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentSearcher<K, V> implements CloseableResource {

    private final Logger logger = LoggerFactory.getLogger(Segment.class);
    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentConf segmentConf;
    private final OptimisticLockObjectVersionProvider versionProvider;

    private SegmentSearcherCore<K, V> searcherCore;
    private OptimisticLock lock;

    public SegmentSearcher(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final OptimisticLockObjectVersionProvider versionProvider) {
        logger.debug("Opening segment '{}' searched", segmentFiles.getId());
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.segmentConf = Objects.requireNonNull(segmentConf);
        this.versionProvider = Objects.requireNonNull(versionProvider);
        optionallyrefreshCoreSearcher();
    }

    private void optionallyrefreshCoreSearcher() {
        if (lock == null) {
            lock = new OptimisticLock(versionProvider);
            optionallyCloseSearcherCore();
        }
        if (lock.isLocked()) {
            optionallyCloseSearcherCore();
        }
        if (searcherCore == null) {
            logger.debug("Opening segment '{}' searched", segmentFiles.getId());
            searcherCore = new SegmentSearcherCore<>(segmentFiles, segmentConf);
        }
    }

    private void optionallyCloseSearcherCore() {
        if (searcherCore == null) {
            return;
        }
        searcherCore.getBloomFilter().logStats();
        logger.debug("Closing segment '{}' searcher", segmentFiles.getId());
        searcherCore = null;
    }

    public K getMaxKey() {
        optionallyrefreshCoreSearcher();
        return searcherCore.getMaxKey();
    }

    public K getMinKey() {
        optionallyrefreshCoreSearcher();
        return searcherCore.getMinKey();
    }

    public V get(final K key) {
        optionallyrefreshCoreSearcher();
        return searcherCore.get(key);
    }

    @Override
    public void close() {
        optionallyCloseSearcherCore();
    }

}
