package org.hestiastore.index.scarceindex;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.sorteddatafile.SortedDataFileWriter;

/**
 * Encapsulate writing of new index data. When writer is closed cache is
 * refreshed from disk.
 */
public class ScarceIndexWriter<K> implements PairWriter<K, Integer> {

    private final ScarceIndex<K> scarceIndex;
    private final SortedDataFileWriter<K, Integer> writer;

    ScarceIndexWriter(final ScarceIndex<K> scarceIndex,
            final SortedDataFileWriter<K, Integer> writer) {
        this.scarceIndex = Vldtn.requireNonNull(scarceIndex, "scarceIndex");
        this.writer = Vldtn.requireNonNull(writer, "writer");
    }

    @Override
    public void put(final Pair<K, Integer> pair) {
        writer.write(pair);
    }

    @Override
    public void close() {
        writer.close();
        scarceIndex.loadCache();
    }

}
