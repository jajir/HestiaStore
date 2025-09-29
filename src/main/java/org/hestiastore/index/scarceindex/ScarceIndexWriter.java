package org.hestiastore.index.scarceindex;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.sorteddatafile.SortedDataFileWriterTx;

/**
 * Encapsulate writing of new index data. When writer is closed cache is
 * refreshed from disk.
 */
public class ScarceIndexWriter<K> implements PairWriter<K, Integer> {

    private final ScarceIndex<K> scarceIndex;
    private final SortedDataFileWriterTx<K, Integer> writerTx;
    private final PairWriter<K, Integer> writer;

    ScarceIndexWriter(final ScarceIndex<K> scarceIndex,
            final SortedDataFileWriterTx<K, Integer> writerTx) {
        this.scarceIndex = Vldtn.requireNonNull(scarceIndex, "scarceIndex");
        this.writerTx = Vldtn.requireNonNull(writerTx, "writerTx");
        this.writer = writerTx.openWriter();
    }

    @Override
    public void put(final Pair<K, Integer> pair) {
        writer.put(pair);
    }

    @Override
    public void close() {
        writer.close();
        writerTx.commit();
        scarceIndex.loadCache();
    }

}
