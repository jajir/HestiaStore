package org.hestiastore.index.scarceindex;

import org.hestiastore.index.GuardedPairWriter;
import org.hestiastore.index.GuardedWriteTransaction;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.sorteddatafile.SortedDataFileWriterTx;

public class ScarceIndexWriterTx<K>
        extends GuardedWriteTransaction<PairWriter<K, Integer>>
        implements WriteTransaction<K, Integer> {

    private final ScarceIndex<K> scarceIndex;
    private final SortedDataFileWriterTx<K, Integer> writerTx;

    ScarceIndexWriterTx(final ScarceIndex<K> scarceIndex,
            final SortedDataFileWriterTx<K, Integer> writerTx) {
        this.scarceIndex = Vldtn.requireNonNull(scarceIndex, "scarceIndex");
        this.writerTx = Vldtn.requireNonNull(writerTx, "writerTx");
    }

    @Override
    protected PairWriter<K, Integer> doOpen() {
        return new GuardedPairWriter<>(writerTx.open());
    }

    @Override
    protected void doCommit(final PairWriter<K, Integer> writer) {
        writerTx.commit();
        scarceIndex.loadCache();
    }
}
