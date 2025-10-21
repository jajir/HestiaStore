package org.hestiastore.index.segment;

import org.hestiastore.index.GuardedWriteTransaction;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.chunkpairfile.ChunkPairFileWriterTx;
import org.hestiastore.index.scarceindex.ScarceIndexWriterTx;

public class SegmentFullWriterTx<K, V>
        extends GuardedWriteTransaction<PairWriter<K, V>>
        implements WriteTransaction<K, V> {

    private final SegmentPropertiesManager segmentPropertiesManager;
    private final int maxNumberOfKeysInIndexPage;
    private final SegmentDataProvider<K, V> segmentDataProvider;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;
    private final ChunkPairFileWriterTx<K, V> chunkPairFileWriterTx;
    private final ScarceIndexWriterTx<K> scarceIndexWriterTx;
    private SegmentFullWriter<K, V> segmentFullWriter;

    SegmentFullWriterTx(final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager propertiesManager,
            final int maxNumberOfKeysInIndexPage,
            final SegmentDataProvider<K, V> dataProvider,
            final SegmentDeltaCacheController<K, V> deltaCacheController) {
        this.segmentPropertiesManager = Vldtn.requireNonNull(propertiesManager,
                "segmentPropertiesManager");
        this.maxNumberOfKeysInIndexPage = maxNumberOfKeysInIndexPage;
        this.segmentDataProvider = Vldtn.requireNonNull(dataProvider,
                "segmentCacheDataProvider");
        this.deltaCacheController = Vldtn.requireNonNull(deltaCacheController,
                "deltaCacheController");
        this.chunkPairFileWriterTx = segmentFiles.getIndexFile().openWriterTx();
        this.scarceIndexWriterTx = segmentFiles.getScarceIndex().openWriterTx();
    }

    @Override
    protected PairWriter<K, V> doOpen() {
        final PairWriter<K, Integer> scarceWriter = scarceIndexWriterTx.open();
        segmentFullWriter = new SegmentFullWriter<>(maxNumberOfKeysInIndexPage,
                segmentDataProvider, chunkPairFileWriterTx.openWriter(),
                scarceWriter);
        return segmentFullWriter;
    }

    @Override
    protected void doCommit(final PairWriter<K, V> writer) {
        scarceIndexWriterTx.commit();
        chunkPairFileWriterTx.commit();
        deltaCacheController.clear();

        segmentPropertiesManager.setNumberOfKeysInCache(0);
        segmentPropertiesManager
                .setNumberOfKeysInIndex(segmentFullWriter.getNumberKeys());
        segmentPropertiesManager.setNumberOfKeysInScarceIndex(
                segmentFullWriter.getNumberKeysInScarceIndex());
    }
}
