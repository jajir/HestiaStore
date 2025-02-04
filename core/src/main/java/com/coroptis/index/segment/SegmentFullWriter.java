package com.coroptis.index.segment;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.bloomfilter.BloomFilterWriter;
import com.coroptis.index.scarceindex.ScarceIndexWriter;
import com.coroptis.index.sorteddatafile.SortedDataFileWriter;

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
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final int maxNumberOfKeysInIndexPage;

    private final AtomicLong scarceIndexKeyCounter = new AtomicLong(0L);
    private final AtomicLong keyCounter = new AtomicLong(0L);
    private final ScarceIndexWriter<K> scarceWriter;
    private final SortedDataFileWriter<K, V> indexWriter;
    private final BloomFilterWriter<K> bloomFilterWriter;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;
    private Pair<K, V> previousPair = null;

    SegmentFullWriter(final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentStatsManager,
            final int maxNumberOfKeysInIndexPage,
            final SegmentDataProvider<K, V> segmentCacheDataProvider,
            final SegmentDeltaCacheController<K, V> deltaCacheController) {
        this.maxNumberOfKeysInIndexPage = Objects
                .requireNonNull(maxNumberOfKeysInIndexPage);
        this.segmentPropertiesManager = Objects
                .requireNonNull(segmentStatsManager);
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.scarceWriter = segmentFiles.getTempScarceIndex().openWriter();
        this.indexWriter = segmentFiles.getTempIndexFile().openWriter();
        Objects.requireNonNull(segmentCacheDataProvider,
                "Segment cached data provider is required");
        this.deltaCacheController = Objects
                .requireNonNull(deltaCacheController);
        segmentCacheDataProvider.invalidate();
        bloomFilterWriter = Objects.requireNonNull(
                segmentCacheDataProvider.getBloomFilter().openWriter());
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
                final long position = indexWriter.put(previousPair, true);
                scarceWriter
                        .put(Pair.of(previousPair.getKey(), (int) position));
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
            final long position = indexWriter.put(previousPair, true);
            scarceWriter.put(Pair.of(previousPair.getKey(), (int) position));
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

        deltaCacheController.clear();

        // update segment statistics
        segmentPropertiesManager.setNumberOfKeysInCache(0);
        segmentPropertiesManager.setNumberOfKeysInIndex(keyCounter.get());
        segmentPropertiesManager
                .setNumberOfKeysInScarceIndex(scarceIndexKeyCounter.get());
        segmentPropertiesManager.flush();
    }

}
