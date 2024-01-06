package com.coroptis.index.segment;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.OptimisticLockObjectVersionProvider;
import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairReader;
import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.bloomfilter.BloomFilterWriter;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.scarceindex.ScarceIndex;
import com.coroptis.index.segmentcache.SegmentCache;
import com.coroptis.index.sstfile.SstFileWriter;

/**
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class Segment<K, V>
        implements CloseableResource, OptimisticLockObjectVersionProvider {

    private final Logger logger = LoggerFactory.getLogger(Segment.class);
    private final long maxNumberOfKeysInSegmentCache;
    private final SegmentCache<K, V> cache;
    private final ScarceIndex<K> scarceIndex;
    private final SegmentStatsManager segmentStatsManager;
    private final int maxNumberOfKeysInIndexPage;
    private final int bloomFilterNumberOfHashFunctions;
    private final int bloomFilterIndexSizeInBytes;
    private final BloomFilter<K> bloomFilter;
    private final SegmentFiles<K, V> segmentFiles;
    private final VersionController versionController;

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
        this.maxNumberOfKeysInSegmentCache = maxNumeberOfKeysInSegmentCache;
        this.cache = new SegmentCache<>(keyTypeDescriptor,segmentFiles);
        this.scarceIndex = ScarceIndex.<K>builder().withDirectory(directory)
                .withFileName(segmentFiles.getScarceFileName())
                .withKeyTypeDescriptor(keyTypeDescriptor).build();
        this.segmentStatsManager = new SegmentStatsManager(directory, id);
        this.maxNumberOfKeysInIndexPage = maxNumberOfKeysInIndexPage;
        this.bloomFilter = BloomFilter.<K>builder()
                .withBloomFilterFileName(segmentFiles.getBloomFilterFileName())
                .withConvertorToBytes(keyTypeDescriptor.getConvertorToBytes())
                .withDirectory(directory)
                .withIndexSizeInBytes(bloomFilterIndexSizeInBytes)
                .withNumberOfHashFunctions(bloomFilterNumberOfHashFunctions)
                .build();
        this.versionController = Objects.requireNonNull(versionController,
                "Version controller is required");
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
    }

    public K getMaxKey() {
        return scarceIndex.getMaxKey();
    }

    public K getMinKey() {
        return scarceIndex.getMinKey();
    }

    int getMaxNumberOfKeysInIndexPage() {
        return maxNumberOfKeysInIndexPage;
    }

    public SegmentStats getStats() {
        return segmentStatsManager.getSegmentStats();
    }

    public SegmentCache<K, V> getCache() {
        return cache;
    }

    /**
     * It's called after writing is done. It ensure that all data are stored in
     * directory.
     */
    public void flush() {
        if (cache.size() > maxNumberOfKeysInSegmentCache) {
            forceCompact();
        } else {
            flushCache();
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

    private void flushCache() {
        segmentStatsManager.setNumberOfKeysInCache(cache.flushCache());
        segmentStatsManager.flush();
    }

    void optionallyCompact() {
        if (cache.size() > maxNumberOfKeysInSegmentCache) {
            forceCompact();
        }
    }

    public PairIterator<K, V> openIterator() {
        return new MergeIterator<K, V>(
                segmentFiles.getIndexSstFile().openIterator(this),
                getCache().getSortedIterator(),
                segmentFiles.getKeyTypeDescriptor(),
                segmentFiles.getValueTypeDescriptor());
    }

    public void forceCompact() {
        versionController.changeVersion();
        try (final SegmentFullWriter<K, V> writer = openFullWriter()) {
            try (final PairIterator<K, V> iterator = openIterator()) {
                while (iterator.hasNext()) {
                    writer.put(iterator.next());
                }
            }
        }
    }

    void finishFullWrite(final long numberOfKeysInMainIndex) {
        cache.clear();
        flushCache();
        segmentFiles.getDirectory().renameFile(
                segmentFiles.getTempIndexFileName(),
                segmentFiles.getIndexFileName());
        segmentFiles.getDirectory().renameFile(
                segmentFiles.getTempScarceFileName(),
                segmentFiles.getScarceFileName());
        scarceIndex.loadCache();
        segmentStatsManager.setNumberOfKeysInCache(0);
        segmentStatsManager.setNumberOfKeysInIndex(numberOfKeysInMainIndex);
        segmentStatsManager
                .setNumberOfKeysInScarceIndex(scarceIndex.getKeyCount());
        segmentStatsManager.flush();
    }

    /**
     * Method should be called just from inside of this package. Method open
     * direct writer to scarce index and main sst file. It's useful for
     * compacting.
     */
    SegmentFullWriter<K, V> openFullWriter() {
        return new SegmentFullWriter<K, V>(this, segmentFiles);
    }

    public SegmentWriter<K, V> openWriter() {
        return new SegmentWriter<>(this);
    }

    public BloomFilterWriter<K> openBloomFilterWriter() {
        return bloomFilter.openWriter();
    }

    public V get(final K key) {
        // look in cache
        final V out = cache.get(key);
        if (segmentFiles.getValueTypeDescriptor().isTombstone(out)) {
            return null;
        }

        // look in bloom filter
        if (out == null) {
            if (bloomFilter.isNotStored(key)) {
                /*
                 * It;s sure that key is not in index.
                 */
                return null;
            }
        }

        // look in index file
        if (out == null) {
            final Integer position = scarceIndex.get(key);
            if (position == null) {
                return null;
            }
            try (final PairReader<K, V> fileReader = segmentFiles
                    .getIndexSstFile().openReader(position)) {
                for (int i = 0; i < getMaxNumberOfKeysInIndexPage(); i++) {
                    final Pair<K, V> pair = fileReader.read();
                    final int cmp = segmentFiles.getKeyTypeDescriptor()
                            .getComparator().compare(pair.getKey(), key);
                    if (cmp == 0) {
                        return pair.getValue();
                    }
                    /**
                     * Keys are in ascending order. When searched key is smaller
                     * than key read from sorted data than key is not found.
                     */
                    if (cmp > 0) {
                        return null;
                    }
                }
            }
        }
        return out;
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

            try (final SegmentFullWriter<K, V> writer = openFullWriter()) {
                while (iterator.hasNext()) {
                    final Pair<K, V> pair = iterator.next();
                    writer.put(pair);
                }
            }
        }

        return lowerSegment;
    }

    @Override
    public void close() {
        bloomFilter.logStats();
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
