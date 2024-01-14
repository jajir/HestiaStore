package com.coroptis.index.segment;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.OptimisticLock;
import com.coroptis.index.OptimisticLockObjectVersionProvider;
import com.coroptis.index.Pair;

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
    private final SegmentPropertiesController segmentPropertiesController;

    private SegmentSearcherCore<K, V> searcherCore;
    private OptimisticLock lock;

    public SegmentSearcher(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final OptimisticLockObjectVersionProvider versionProvider,
            final SegmentPropertiesController segmentPropertiesController) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.segmentConf = Objects.requireNonNull(segmentConf);
        this.versionProvider = Objects.requireNonNull(versionProvider);
        this.segmentPropertiesController = Objects
                .requireNonNull(segmentPropertiesController);
        optionallyrefreshCoreSearcher();
    }

    /**
     * This method allows into in memory segment searched add some data.
     * 
     * @param pair
     */
    void addPairIntoCache(final Pair<K, V> pair) {
        if (searcherCore == null) {
            return;
        }
        searcherCore.addPairIntoCache(pair);
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
            logger.debug("Opening searcher for segment '{}'",
                    segmentFiles.getId());
            searcherCore = new SegmentSearcherCore<>(segmentFiles, segmentConf,
                    segmentPropertiesController.getSegmentPropertiesManager());
        }
    }

    private void optionallyCloseSearcherCore() {
        if (searcherCore == null) {
            return;
        }
        searcherCore.getBloomFilter().logStats();
        logger.debug("Closing searcher for segment '{}'", segmentFiles.getId());
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
