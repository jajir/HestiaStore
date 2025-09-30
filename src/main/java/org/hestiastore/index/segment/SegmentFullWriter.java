package org.hestiastore.index.segment;

import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bloomfilter.BloomFilterWriter;
import org.hestiastore.index.chunkpairfile.ChunkPairFileWriter;
import org.hestiastore.index.chunkstore.CellPosition;
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
 * 
 * @param <K> key type
 * @param <V> value type
 */
public class SegmentFullWriter<K, V> implements PairWriter<K, V> {

    private final int maxNumberOfKeysInIndexPage;

    private final AtomicLong scarceIndexKeyCounter = new AtomicLong(0L);
    private final AtomicLong keyCounter = new AtomicLong(0L);
    private final ScarceIndexWriter<K> scarceWriter;
    private final ChunkPairFileWriter<K, V> indexWriter;
    private final BloomFilterWriter<K> bloomFilterWriter;
    private Pair<K, V> lastPair = null;

    SegmentFullWriter(final SegmentFiles<K, V> segmentFiles,
            final int maxNumberOfKeysInIndexPage,
            final SegmentDataProvider<K, V> segmentCacheDataProvider,
            final ChunkPairFileWriter<K, V> chunkPairFileWriter) {
        this.maxNumberOfKeysInIndexPage = Vldtn.requireNonNull(
                maxNumberOfKeysInIndexPage, "maxNumberOfKeysInIndexPage");
        this.scarceWriter = segmentFiles.getTempScarceIndex().openWriter();
        this.indexWriter = Vldtn.requireNonNull(chunkPairFileWriter,
                "indexWriter");
        Vldtn.requireNonNull(segmentCacheDataProvider,
                "segmentCacheDataProvider");
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
        final CellPosition position = indexWriter.flush();
        scarceWriter.write(Pair.of(lastPair.getKey(), position.getValue()));
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

    public long getNumberKeys() {
        return keyCounter.get();
    }

    public long getNumberKeysInScarceIndex() {
        return scarceIndexKeyCounter.get();
    }

}
