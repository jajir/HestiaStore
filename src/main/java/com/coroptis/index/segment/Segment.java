package com.coroptis.index.segment;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairReader;
import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.bloomfilter.BloomFilterWriter;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.scarceindex.ScarceIndex;
import com.coroptis.index.sstfile.SstFile;
import com.coroptis.index.sstfile.SstFileStreamer;
import com.coroptis.index.sstfile.SstFileWriter;

/**
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class Segment<K, V> implements CloseableResource {

    private final static String INDEX_FILE_NAME_EXTENSION = ".index";
    private final static String SCARCE_FILE_NAME_EXTENSION = ".scarce";
    private final static String CACHE_FILE_NAME_EXTENSION = ".cache";
    private final static String TEMP_FILE_NAME_EXTENSION = ".tmp";
    private final static String BOOM_FILTER_FILE_NAME_EXTENSION = ".bloom-filter";
    private final Directory directory;
    private final SegmentId id;
    private final long maxNumberOfKeysInSegmentCache;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final UniqueCache<K, V> cache;
    private final ScarceIndex<K> scarceIndex;
    private final SegmentStatsManager segmentStatsManager;
    private final int maxNumberOfKeysInIndexPage;
    private final int bloomFilterNumberOfHashFunctions;
    private final int bloomFilterIndexSizeInBytes;
    private final BloomFilter<K> bloomFilter;

    public static <M, N> SegmentBuilder<M, N> builder() {
        return new SegmentBuilder<>();
    }

    public Segment(final Directory directory, final SegmentId id,
            final long maxNumeberOfKeysInSegmentCache,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int maxNumberOfKeysInIndexPage,
            final int bloomFilterNumberOfHashFunctions,
            final int bloomFilterIndexSizeInBytes) {
        this.directory = Objects.requireNonNull(directory);
        this.id = Objects.requireNonNull(id);
        this.maxNumberOfKeysInSegmentCache = maxNumeberOfKeysInSegmentCache;
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        this.cache = loadCache();
        this.scarceIndex = ScarceIndex.<K>builder().withDirectory(directory)
                .withFileName(getScarceFileName())
                .withKeyTypeDescriptor(keyTypeDescriptor).build();
        this.segmentStatsManager = new SegmentStatsManager(directory, id);
        this.maxNumberOfKeysInIndexPage = maxNumberOfKeysInIndexPage;
        this.bloomFilter = BloomFilter.<K>builder()
                .withBloomFilterFileName(getBloomFilterFileName())
                .withConvertorToBytes(keyTypeDescriptor.getConvertorToBytes())
                .withDirectory(directory)
                .withIndexSizeInBytes(bloomFilterIndexSizeInBytes)
                .withNumberOfHashFunctions(bloomFilterNumberOfHashFunctions)
                .build();
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
    }

    private UniqueCache<K, V> loadCache() {
        final UniqueCache<K, V> out = new UniqueCache<>(
                keyTypeDescriptor.getComparator());
        try (final SstFileStreamer<K, V> fileStreamer = getCacheSstFile()
                .openStreamer()) {
            fileStreamer.stream().forEach(pair -> out.put(pair));
        }
        return out;
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

    private String getCacheFileName() {
        return id.getName() + CACHE_FILE_NAME_EXTENSION;
    }

    private String getTempIndexFileName() {
        return id.getName() + TEMP_FILE_NAME_EXTENSION
                + INDEX_FILE_NAME_EXTENSION;
    }

    private String getTempScarceFileName() {
        return id.getName() + TEMP_FILE_NAME_EXTENSION
                + SCARCE_FILE_NAME_EXTENSION;
    }

    private String getScarceFileName() {
        return id.getName() + SCARCE_FILE_NAME_EXTENSION;
    }

    private String getBloomFilterFileName() {
        return id.getName() + BOOM_FILTER_FILE_NAME_EXTENSION;
    }

    private String getIndexFileName() {
        return id.getName() + INDEX_FILE_NAME_EXTENSION;
    }

    public SegmentStats getStats() {
        return segmentStatsManager.getSegmentStats();
    }

    private SstFile<K, V> getCacheSstFile() {
        return new SstFile<>(directory, getCacheFileName(),
                valueTypeDescriptor.getTypeWriter(),
                valueTypeDescriptor.getTypeReader(),
                keyTypeDescriptor.getComparator(),
                keyTypeDescriptor.getConvertorFromBytes(),
                keyTypeDescriptor.getConvertorToBytes());
    }

    private SstFile<K, V> getIndexSstFile() {
        return new SstFile<>(directory, getIndexFileName(),
                valueTypeDescriptor.getTypeWriter(),
                valueTypeDescriptor.getTypeReader(),
                keyTypeDescriptor.getComparator(),
                keyTypeDescriptor.getConvertorFromBytes(),
                keyTypeDescriptor.getConvertorToBytes());
    }

    SstFile<K, V> getTempIndexFile() {
        return new SstFile<>(directory, getTempIndexFileName(),
                valueTypeDescriptor.getTypeWriter(),
                valueTypeDescriptor.getTypeReader(),
                keyTypeDescriptor.getComparator(),
                keyTypeDescriptor.getConvertorFromBytes(),
                keyTypeDescriptor.getConvertorToBytes());
    }

    UniqueCache<K, V> getCache() {
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
        if (!directory.isFileExists(getScarceFileName())) {
            directory.touch(getScarceFileName());
        }
        if (!directory.isFileExists(getIndexFileName())) {
            directory.touch(getIndexFileName());
        }
    }

    private void flushCache() {
        final AtomicLong cx = new AtomicLong(0);
        try (final SstFileWriter<K, V> writer = getCacheSstFile()
                .openWriter()) {
            cache.getStream().forEach(pair -> {
                writer.put(pair);
                cx.incrementAndGet();
            });
        }
        segmentStatsManager.setNumberOfKeysInCache(cx.get());
        segmentStatsManager.flush();
    }

    void optionallyCompact() {
        if (cache.size() > maxNumberOfKeysInSegmentCache) {
            forceCompact();
        }
    }

    public Stream<Pair<K, V>> getStream() {
        return StreamSupport
                .stream(new MergeSpliterator<>(getIndexSstFile().openIterator(),
                        getCache().getSortedIterator(), keyTypeDescriptor,
                        valueTypeDescriptor), false);
    }

    public PairIterator<K, V> openIterator() {
        return new MergeIterator<K, V>(getIndexSstFile().openIterator(),
                getCache().getSortedIterator(), keyTypeDescriptor,
                valueTypeDescriptor);
    }

    ScarceIndex<K> getTempScarceIndex() {
        return ScarceIndex.<K>builder().withDirectory(directory)
                .withFileName(getTempScarceFileName())
                .withKeyTypeDescriptor(keyTypeDescriptor).build();
    }

    public void forceCompact() {
        try (final SegmentFullWriter<K, V> writer = openFullWriter()) {
            getStream().forEach(writer::put);
        }
    }

    void finishFullWrite(final long numberOfKeysInMainIndex) {
        cache.clear();
        flushCache();
        directory.renameFile(getTempIndexFileName(), getIndexFileName());
        directory.renameFile(getTempScarceFileName(), getScarceFileName());
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
        return new SegmentFullWriter<K, V>(this);
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
        if (valueTypeDescriptor.isTombstone(out)) {
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
            try (final PairReader<K, V> fileReader = getIndexSstFile()
                    .openReader(position)) {
                for (int i = 0; i < getMaxNumberOfKeysInIndexPage(); i++) {
                    final Pair<K, V> pair = fileReader.read();
                    final int cmp = keyTypeDescriptor.getComparator()
                            .compare(pair.getKey(), key);
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
        long cx = 0;
        long half = getStats().getNumberOfKeys() / 2;

        final Segment<K, V> lowerSegment = Segment.<K, V>builder()
                .withDirectory(directory).withId(segmentId)
                .withKeyTypeDescriptor(this.keyTypeDescriptor)
                .withValueTypeDescriptor(this.valueTypeDescriptor)
                .withMaxNumberOfKeysInSegmentCache(
                        maxNumberOfKeysInSegmentCache)
                .withMaxNumberOfKeysInIndexPage(maxNumberOfKeysInIndexPage)
                .withBloomFilterIndexSizeInBytes(bloomFilterIndexSizeInBytes)
                .withBloomFilterNumberOfHashFunctions(
                        bloomFilterNumberOfHashFunctions)
                .build();
        final Iterable<Pair<K, V>> iterable = getStream()::iterator;
        final Iterator<Pair<K, V>> iterator = iterable.iterator();
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

        return lowerSegment;
    }

    @Override
    public void close() {
        //Do intentionally nothing.
    }

    public SegmentId getId() {
        return id;
    }

}
