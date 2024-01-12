package com.coroptis.index.segment;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.bloomfilter.BloomFilterWriter;
import com.coroptis.index.scarceindex.ScarceIndexWriter;
import com.coroptis.index.sstfile.SstFileWriter;

/**
 * Allows to rewrite whole segment context including:
 * <ul>
 * <li>Main segment SST file</li>
 * <li>Scarce index</li>
 * <li>Build new bloom filter</li>
 * <li>clean cache</li>
 * <ul>
 * .
 */
public class SegmentFullWriter<K, V> implements PairWriter<K, V> {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentPropertiesController segmentStatsController;
    private final int maxNumberOfKeysInIndexPage;

    private final AtomicLong scarceIndexKeyCounter = new AtomicLong(0L);
    private final AtomicLong keyCounter = new AtomicLong(0L);
    private final ScarceIndexWriter<K> scarceWriter;
    private final SstFileWriter<K, V> indexWriter;
    private final BloomFilterWriter<K> bloomFilterWriter;
    private Pair<K, V> previousPair = null;

    SegmentFullWriter(final BloomFilter<K> bloomFilter,
            final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesController segmentStatsController,
            final int maxNumberOfKeysInIndexPage) {
        this.maxNumberOfKeysInIndexPage = Objects
                .requireNonNull(maxNumberOfKeysInIndexPage);
        this.segmentStatsController = Objects
                .requireNonNull(segmentStatsController);
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.scarceWriter = segmentFiles.getTempScarceIndex().openWriter();
        this.indexWriter = segmentFiles.getTempIndexFile().openWriter();
        bloomFilterWriter = Objects.requireNonNull(bloomFilter.openWriter());
    }

    @Override
    public void put(final Pair<K, V> pair) {
        Objects.requireNonNull(pair);

        bloomFilterWriter.write(pair.getKey());

        if (previousPair != null) {
            final long i = keyCounter.getAndIncrement();
            /*
             * Write first pair end every nth pair.
             */
            if (i % maxNumberOfKeysInIndexPage == 0) {
                final int position = indexWriter.put(previousPair, true);
                scarceWriter.put(Pair.of(previousPair.getKey(), position));
                scarceIndexKeyCounter.incrementAndGet();
            } else {
                indexWriter.put(previousPair);
            }
        }

        previousPair = pair;
    }

    @Override
    public void close() {
        if (previousPair != null) {
            // write last pair to scarce index
            final int position = indexWriter.put(previousPair, true);
            scarceWriter.put(Pair.of(previousPair.getKey(), position));
            keyCounter.getAndIncrement();
            scarceIndexKeyCounter.incrementAndGet();
        }
        // close all resources
        scarceWriter.close();
        indexWriter.close();
        bloomFilterWriter.close();

        // rename temporal files to main one
        segmentFiles.getDirectory().renameFile(
                segmentFiles.getTempIndexFileName(),
                segmentFiles.getIndexFileName());
        segmentFiles.getDirectory().renameFile(
                segmentFiles.getTempScarceFileName(),
                segmentFiles.getScarceFileName());

        // clean cache
        final SegmentCache<K, V> sc = new SegmentCache<>(
                segmentFiles.getKeyTypeDescriptor(), segmentFiles,
                segmentStatsController.getSegmentPropertiesManager());
        sc.clear();
        sc.flushCache();

        // update segment statistics
        final SegmentPropertiesManager segmenPropertiesManager = segmentStatsController
                .getSegmentPropertiesManager();
        segmenPropertiesManager.setNumberOfKeysInCache(0);
        segmenPropertiesManager.setNumberOfKeysInIndex(keyCounter.get());
        segmenPropertiesManager
                .setNumberOfKeysInScarceIndex(scarceIndexKeyCounter.get());
        segmenPropertiesManager.flush();

    }

}
