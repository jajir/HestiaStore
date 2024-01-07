package com.coroptis.index.segment;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.OptimisticLockObjectVersionProvider;
import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairWriter;
import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.scarceindex.ScarceIndex;
import com.coroptis.index.segmentcache.SegmentCache;

/**
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class Segment<K, V> implements CloseableResource, SegmentCompacter<K, V>,
        OptimisticLockObjectVersionProvider {

    private final Logger logger = LoggerFactory.getLogger(Segment.class);
    private final Directory directory;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final long maxNumberOfKeysInSegmentCache;
    private final int maxNumberOfKeysInIndexPage;
    private final int bloomFilterNumberOfHashFunctions;
    private final int bloomFilterIndexSizeInBytes;
    private final SegmentFiles<K, V> segmentFiles;
    private final VersionController versionController;
    private final SegmentSearcherManager<K, V> segmentSearcherManager;
    private final SegmentStatsController segmentStatsController;

    public static <M, N> SegmentBuilder<M, N> builder() {
        return new SegmentBuilder<>();
    }

    public Segment(final Directory directory, final SegmentId id,
            final long maxNumeberOfKeysInSegmentCache,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int maxNumberOfKeysInIndexPage,
            final int bloomFilterNumberOfHashFunctions,
            final int bloomFilterIndexSizeInBytes,
            VersionController versionController) {
        this.segmentFiles = new SegmentFiles<>(directory, id, keyTypeDescriptor,
                valueTypeDescriptor);
        logger.debug("Initializing segment '{}'", segmentFiles.getId());
        this.directory = Objects.requireNonNull(directory);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.maxNumberOfKeysInSegmentCache = maxNumeberOfKeysInSegmentCache;
        this.maxNumberOfKeysInIndexPage = maxNumberOfKeysInIndexPage;
        this.versionController = Objects.requireNonNull(versionController,
                "Version controller is required");
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;

        this.segmentStatsController = new SegmentStatsController(directory, id,
                versionController);

        this.segmentSearcherManager = new SegmentSearcherManager<>(directory,
                id, keyTypeDescriptor, valueTypeDescriptor,
                maxNumberOfKeysInIndexPage, bloomFilterNumberOfHashFunctions,
                bloomFilterIndexSizeInBytes, versionController);
    }

    private ScarceIndex<K> getScarceIndex() {
        return ScarceIndex.<K>builder().withDirectory(directory)
                .withFileName(segmentFiles.getScarceFileName())
                .withKeyTypeDescriptor(keyTypeDescriptor).build();
    }

    // FIXME remove it
    @Deprecated
    public K getMaxKey() {
        return getScarceIndex().getMaxKey();
    }

    // FIXME remove it
    @Deprecated
    public K getMinKey() {
        return getScarceIndex().getMinKey();
    }

    public SegmentStats getStats() {
        return segmentStatsController.getSegmentStatsManager()
                .getSegmentStats();
    }

    private SegmentCache<K, V> makeCache() {
        return new SegmentCache<>(keyTypeDescriptor, segmentFiles);
    }

    /**
     * It's called after writing is done. It ensure that all data are stored in
     * directory.
     */
    public void flush() {
        final SegmentCache<K, V> sc = makeCache();
        if (sc.size() > maxNumberOfKeysInSegmentCache) {
            forceCompact();
        } else {
            flushCache(sc);
        }
        if (!segmentFiles.getDirectory()
                .isFileExists(segmentFiles.getScarceFileName())) {
            segmentFiles.getDirectory().touch(segmentFiles.getScarceFileName());
        }
        if (!segmentFiles.getDirectory()
                .isFileExists(segmentFiles.getIndexFileName())) {
            segmentFiles.getDirectory().touch(segmentFiles.getIndexFileName());
        }
    }

    private void flushCache(final SegmentCache<K, V> segmentCache) {
        SegmentStatsManager segmentStatsManager = segmentStatsController
                .getSegmentStatsManager();
        segmentStatsManager.setNumberOfKeysInCache(segmentCache.flushCache());
        segmentStatsManager.flush();
    }

    @Override
    public void optionallyCompact() {
        if (segmentStatsController.getSegmentStatsManager().getSegmentStats()
                .getNumberOfKeysInCache() > maxNumberOfKeysInSegmentCache) {
            forceCompact();
        }
    }

    public PairIterator<K, V> openIterator() {
        // TODO this naive implementation ignores possible in memory cache.
        return new SegmentReader<>(segmentFiles)
                .openIterator(versionController);
    }

    @Override
    public void forceCompact() {
        versionController.changeVersion();
        long cx = 0;
        try (final SegmentFullWriter<K, V> writer = openFullWriter()) {
            try (final PairIterator<K, V> iterator = openIterator()) {
                while (iterator.hasNext()) {
                    writer.put(iterator.next());
                    cx++;
                }
            }
        }
        finishFullWrite(cx);
    }

    private void finishFullWrite(final long numberOfKeysInMainIndex) {
        final SegmentCache<K, V> sc = makeCache();
        sc.clear();
        flushCache(sc);
        segmentFiles.getDirectory().renameFile(
                segmentFiles.getTempIndexFileName(),
                segmentFiles.getIndexFileName());
        segmentFiles.getDirectory().renameFile(
                segmentFiles.getTempScarceFileName(),
                segmentFiles.getScarceFileName());
        SegmentStatsManager segmentStatsManager = segmentStatsController
                .getSegmentStatsManager();
        segmentStatsManager.setNumberOfKeysInCache(0);
        segmentStatsManager.setNumberOfKeysInIndex(numberOfKeysInMainIndex);
        segmentStatsManager
                .setNumberOfKeysInScarceIndex(getScarceIndex().getKeyCount());
        segmentStatsManager.flush();
    }

    /**
     * Method should be called just from inside of this package. Method open
     * direct writer to scarce index and main sst file. It's useful for
     * compacting.
     */
    private SegmentFullWriter<K, V> openFullWriter() {
        final BloomFilter<K> bloomFilter = BloomFilter.<K>builder()
                .withBloomFilterFileName(segmentFiles.getBloomFilterFileName())
                .withConvertorToBytes(keyTypeDescriptor.getConvertorToBytes())
                .withDirectory(directory)
                .withIndexSizeInBytes(bloomFilterIndexSizeInBytes)
                .withNumberOfHashFunctions(bloomFilterNumberOfHashFunctions)
                .build();
        return new SegmentFullWriter<K, V>(bloomFilter, segmentFiles,
                maxNumberOfKeysInIndexPage);
    }

    public PairWriter<K, V> openWriter() {
        SegmentCacheWriterFinal<K, V> writer = new SegmentCacheWriterFinal<>(
                segmentFiles, segmentFiles.getKeyTypeDescriptor(),
                segmentStatsController.getSegmentStatsManager(),
                versionController, this);
        return writer.openWriter();
    }

    public SegmentSearcher<K, V> openSearcher() {
        return segmentSearcherManager.getSearcher();
    }

    public Segment<K, V> split(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId);
        versionController.changeVersion();
        long cx = 0;
        long half = getStats().getNumberOfKeys() / 2;

        final Segment<K, V> lowerSegment = Segment.<K, V>builder()
                .withDirectory(segmentFiles.getDirectory()).withId(segmentId)
                .withKeyTypeDescriptor(segmentFiles.getKeyTypeDescriptor())
                .withValueTypeDescriptor(segmentFiles.getValueTypeDescriptor())
                .withMaxNumberOfKeysInSegmentCache(
                        maxNumberOfKeysInSegmentCache)
                .withMaxNumberOfKeysInIndexPage(maxNumberOfKeysInIndexPage)
                .withBloomFilterIndexSizeInBytes(bloomFilterIndexSizeInBytes)
                .withBloomFilterNumberOfHashFunctions(
                        bloomFilterNumberOfHashFunctions)
                .build();

        try (final PairIterator<K, V> iterator = openIterator()) {

            try (final SegmentFullWriter<K, V> writer = lowerSegment
                    .openFullWriter()) {
                while (cx < half && iterator.hasNext()) {
                    cx++;
                    final Pair<K, V> pair = iterator.next();
                    writer.put(pair);
                }
            }
            lowerSegment.finishFullWrite(cx);

            cx = 0;
            try (final SegmentFullWriter<K, V> writer = openFullWriter()) {
                while (iterator.hasNext()) {
                    cx++;
                    final Pair<K, V> pair = iterator.next();
                    writer.put(pair);
                }
            }
            finishFullWrite(cx);

        }

        return lowerSegment;
    }

    @Override
    public void close() {
//        bloomFilter.logStats();
        logger.debug("Closing segment '{}'", segmentFiles.getId());
        // Do intentionally nothing.
    }

    public SegmentId getId() {
        return segmentFiles.getId();
    }

    @Override
    public int getVersion() {
        return versionController.getVersion();
    }

}
