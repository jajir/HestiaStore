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
@Deprecated
public class SegmentSearcherOL<K, V> implements CloseableResource {

    private final Logger logger = LoggerFactory.getLogger(Segment.class);
    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentConf segmentConf;
    private final OptimisticLockObjectVersionProvider versionProvider;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentIndexSearcherSupplier<K, V> segmentIndexSearcherSupplier;
    private final SegmentDataProvider<K, V> segmentCacheDataProvider;

    private SegmentSearcherCore<K, V> searcherCore;
    private OptimisticLock lock;

    public SegmentSearcherOL(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final OptimisticLockObjectVersionProvider versionProvider,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentIndexSearcherSupplier<K, V> segmentIndexSearcherSupplier,
            final SegmentDataProvider<K, V> segmentCacheDataProvider) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.segmentConf = Objects.requireNonNull(segmentConf);
        this.versionProvider = Objects.requireNonNull(versionProvider);
        this.segmentPropertiesManager = Objects
                .requireNonNull(segmentPropertiesManager);
        this.segmentIndexSearcherSupplier = Objects
                .requireNonNull(segmentIndexSearcherSupplier);
        this.segmentCacheDataProvider = Objects.requireNonNull(
                segmentCacheDataProvider,
                "Segment cached data provider is required");
        optionallyrefreshCoreSearcher();
    }

    private void optionallyrefreshCoreSearcher() {
        if (lock == null) {
            lock = new OptimisticLock(versionProvider);
            optionallyCloseSearcherCore();
        }
        if (lock.isLocked()) {
            logger.debug(
                    "Closing segment searcher '{}', because segment was changed",
                    segmentFiles.getId());
            optionallyCloseSearcherCore();
            lock = new OptimisticLock(versionProvider);
        }
        if (searcherCore == null) {
            logger.debug("Opening segment searcher '{}'", segmentFiles.getId());
            searcherCore = new SegmentSearcherCore<>(segmentFiles, segmentConf,
                    segmentPropertiesManager,
                    segmentIndexSearcherSupplier.get(),
                    segmentCacheDataProvider);
        }
    }

    private void optionallyCloseSearcherCore() {
        if (searcherCore == null) {
            return;
        }
        logger.debug(searcherCore.getBloomFilter().getStatsString());
        logger.debug("Closing segment searcher '{}'", segmentFiles.getId());
        searcherCore = null;
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
