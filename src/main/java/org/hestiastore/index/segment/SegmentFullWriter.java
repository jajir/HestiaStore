package org.hestiastore.index.segment;

import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bloomfilter.BloomFilterWriter;
import org.hestiastore.index.bloomfilter.BloomFilterWriterTx;
import org.hestiastore.index.chunkentryfile.ChunkEntryFileWriter;
import org.hestiastore.index.chunkstore.CellPosition;

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
class SegmentFullWriter<K, V> extends AbstractCloseableResource
        implements EntryWriter<K, V> {

    private final int maxNumberOfKeysInIndexPage;

    private final AtomicLong scarceIndexKeyCounter = new AtomicLong(0L);
    private final AtomicLong keyCounter = new AtomicLong(0L);
    private final EntryWriter<K, Integer> scarceWriter;
    private final ChunkEntryFileWriter<K, V> indexWriter;
    private final BloomFilterWriterTx<K> bloomFilterWriterTx;
    private final BloomFilterWriter<K> bloomFilterWriter;
    private Entry<K, V> lastPair = null;

    /**
     * Creates a writer that rebuilds the full segment content.
     *
     * @param maxNumberOfKeysInIndexPage keys per index page for scarce index
     * @param segmentCacheDataProvider provider for Bloom filter writer
     * @param chunkPairFileWriter writer for the main index file
     * @param scarceWriter writer for the scarce index
     */
    SegmentFullWriter(final int maxNumberOfKeysInIndexPage,
            final SegmentResources<K, V> segmentCacheDataProvider,
            final ChunkEntryFileWriter<K, V> chunkPairFileWriter,
            final EntryWriter<K, Integer> scarceWriter) {
        this.maxNumberOfKeysInIndexPage = Vldtn.requireNonNull(
                maxNumberOfKeysInIndexPage, "maxNumberOfKeysInIndexPage");
        this.scarceWriter = Vldtn.requireNonNull(scarceWriter, "scarceWriter");
        this.indexWriter = Vldtn.requireNonNull(chunkPairFileWriter,
                "indexWriter");
        Vldtn.requireNonNull(segmentCacheDataProvider,
                "segmentCacheDataProvider");
        bloomFilterWriterTx = segmentCacheDataProvider.getBloomFilter()
                .openWriteTx();
        bloomFilterWriter = Vldtn.requireNonNull(bloomFilterWriterTx.open(),
                "bloomFilterWriter");
    }

    /**
     * Writes an entry into the index and Bloom filter, updating sparse index.
     *
     * @param entry entry to write
     */
    @Override
    public void write(final Entry<K, V> entry) {
        Vldtn.requireNonNull(entry, "entry");

        bloomFilterWriter.write(entry.getKey());

        lastPair = entry;

        final long i = keyCounter.getAndIncrement() + 1;
        indexWriter.write(entry);
        /*
         * Write first entry end every nth entry.
         */
        if (i % maxNumberOfKeysInIndexPage == 0) {
            flush();
        }
    }

    /**
     * Flushes a sparse index entry for the most recent key if available.
     */
    private void flush() {
        if (lastPair == null) {
            return;
        }
        final CellPosition position = indexWriter.flush();
        scarceWriter.write(Entry.of(lastPair.getKey(), position.getValue()));
        scarceIndexKeyCounter.incrementAndGet();
        lastPair = null;
    }

    /**
     * Flushes pending sparse entries and closes underlying writers.
     */
    @Override
    protected void doClose() {
        flush();
        // close all resources
        scarceWriter.close();
        indexWriter.close();
        bloomFilterWriter.close();
    }

    /**
     * Commits the Bloom filter writer transaction.
     */
    void commitBloomFilter() {
        bloomFilterWriterTx.commit();
    }

    /**
     * Returns the number of keys written to the main index.
     *
     * @return number of keys written
     */
    public long getNumberKeys() {
        return keyCounter.get();
    }

    /**
     * Returns the number of keys written to the scarce index.
     *
     * @return number of scarce index keys
     */
    public long getNumberKeysInScarceIndex() {
        return scarceIndexKeyCounter.get();
    }

}
