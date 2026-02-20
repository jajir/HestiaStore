package org.hestiastore.index.scarceindex;

import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.GuardedEntryWriter;
import org.hestiastore.index.GuardedWriteTransaction;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.sorteddatafile.SortedDataFileWriterTx;

public class ScarceIndexWriterTx<K>
        extends GuardedWriteTransaction<EntryWriter<K, Integer>>
        implements WriteTransaction<K, Integer> {

    private final ScarceSegmentIndex<K> scarceIndex;
    private final SortedDataFileWriterTx<K, Integer> writerTx;

    ScarceIndexWriterTx(final ScarceSegmentIndex<K> scarceIndex,
            final SortedDataFileWriterTx<K, Integer> writerTx) {
        this.scarceIndex = Vldtn.requireNonNull(scarceIndex, "scarceIndex");
        this.writerTx = Vldtn.requireNonNull(writerTx, "writerTx");
    }

    @Override
    protected EntryWriter<K, Integer> doOpen() {
        return new GuardedEntryWriter<>(writerTx.open());
    }

    @Override
    protected void doCommit(final EntryWriter<K, Integer> writer) {
        writerTx.commit();
        scarceIndex.loadCache();
    }
}
