package org.hestiastore.index.segment;

import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bloomfilter.BloomFilterWriter;
import org.hestiastore.index.chunkpairfile.ChunkPairFileWriter;
import org.hestiastore.index.chunkpairfile.ChunkPairFileWriterTx;
import org.hestiastore.index.scarceindex.ScarceIndexWriter;

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
public class SegmentFullWriterToChunkStore<K, V> implements PairWriter<K, V> {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final int maxNumberOfKeysInIndexPage;

    private final AtomicLong scarceIndexKeyCounter = new AtomicLong(0L);
    private final AtomicLong keyCounter = new AtomicLong(0L);
    private final ScarceIndexWriter<K> scarceWriter;
    private final ChunkPairFileWriterTx<K, V> chunkPairFileWriterTx;
    private final ChunkPairFileWriter<K, V> indexWriter;
    private final BloomFilterWriter<K> bloomFilterWriter;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;
    private Pair<K, V> lastPair = null;

    SegmentFullWriterToChunkStore(final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentStatsManager,
            final int maxNumberOfKeysInIndexPage,
            final SegmentDataProvider<K, V> segmentCacheDataProvider,
            final SegmentDeltaCacheController<K, V> deltaCacheController) {
        this.maxNumberOfKeysInIndexPage = Vldtn.requireNonNull(
                maxNumberOfKeysInIndexPage, "maxNumberOfKeysInIndexPage");
        this.segmentPropertiesManager = Vldtn
                .requireNonNull(segmentStatsManager, "segmentStatsManager");
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.scarceWriter = segmentFiles.getTempScarceIndex().openWriter();
        this.chunkPairFileWriterTx = segmentFiles.getIndexFile().openWriterTx();
        this.indexWriter = chunkPairFileWriterTx.openWriter();
        Vldtn.requireNonNull(segmentCacheDataProvider,
                "segmentCacheDataProvider");
        this.deltaCacheController = Vldtn.requireNonNull(deltaCacheController,
                "deltaCacheController");
        segmentCacheDataProvider.invalidate();
        bloomFilterWriter = Vldtn.requireNonNull(
                segmentCacheDataProvider.getBloomFilter().openWriter(),
                "bloomFilterWriter");
    }

    @Override
    public void write(final Pair<K, V> pair) {
        Vldtn.requireNonNull(pair, "pair");

        bloomFilterWriter.write(pair.getKey());

        lastPair = pair;
        if (lastPair == null) {
        }

        final long i = keyCounter.getAndIncrement() + 1;
        indexWriter.write(pair);
        /*
         * Write first pair end every nth pair.
         */
        if (i % maxNumberOfKeysInIndexPage == 0) {
            flush();
        }
    }

    private void flush() {
        if (lastPair == null) {
            return;
        }
        final long position = indexWriter.flush();
        scarceWriter.write(Pair.of(lastPair.getKey(), (int) position));
        scarceIndexKeyCounter.incrementAndGet();
        lastPair = null;
    }

    @Override
    public void close() {
        flush();
        // close all resources
        scarceWriter.close();
        indexWriter.close();
        bloomFilterWriter.close();
    }

    public void commit() {
        // rename temporal files to main one
        segmentFiles.getDirectory().renameFile(
                segmentFiles.getTempScarceFileName(),
                segmentFiles.getScarceFileName());

        chunkPairFileWriterTx.commit();
        deltaCacheController.clear();

        // update segment statistics
        segmentPropertiesManager.setNumberOfKeysInCache(0);
        segmentPropertiesManager.setNumberOfKeysInIndex(keyCounter.get());
        segmentPropertiesManager
                .setNumberOfKeysInScarceIndex(scarceIndexKeyCounter.get());
        segmentPropertiesManager.flush();
    }

}
