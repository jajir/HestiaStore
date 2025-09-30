package org.hestiastore.index.scarceindex;

import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.sorteddatafile.SortedDataFileWriterTx;

public class ScarceIndexWriterTx<K> implements WriteTransaction<K, Integer> {

    private final ScarceIndex<K> scarceIndex;
    private final SortedDataFileWriterTx<K, Integer> writerTx;

    ScarceIndexWriterTx(final ScarceIndex<K> scarceIndex,
            final SortedDataFileWriterTx<K, Integer> writerTx) {
        this.scarceIndex = Vldtn.requireNonNull(scarceIndex, "scarceIndex");
        this.writerTx = Vldtn.requireNonNull(writerTx, "writerTx");
    }

    @Override
    public PairWriter<K, Integer> openWriter() {
        return writerTx.openWriter();
    }

    @Override
    public void commit() {
        writerTx.commit();
        scarceIndex.loadCache();
    }

}
