package org.hestiastore.index.segment;

import org.hestiastore.index.GuardedWriteTransaction;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.chunkentryfile.ChunkEntryFileWriterTx;
import org.hestiastore.index.scarceindex.ScarceIndexWriterTx;

public class SegmentFullWriterTx<K, V>
        extends GuardedWriteTransaction<EntryWriter<K, V>>
        implements WriteTransaction<K, V> {

    private final SegmentPropertiesManager segmentPropertiesManager;
    private final int maxNumberOfKeysInIndexPage;
    private final SegmentResources<K, V> segmentDataProvider;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;
    private final SegmentCache<K, V> segmentCache;
    private final ChunkEntryFileWriterTx<K, V> chunkPairFileWriterTx;
    private final ScarceIndexWriterTx<K> scarceIndexWriterTx;
    private SegmentFullWriter<K, V> segmentFullWriter;

    SegmentFullWriterTx(final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager propertiesManager,
            final int maxNumberOfKeysInIndexPage,
            final SegmentResources<K, V> dataProvider,
            final SegmentDeltaCacheController<K, V> deltaCacheController,
            final SegmentCache<K, V> segmentCache) {
        this.segmentPropertiesManager = Vldtn.requireNonNull(propertiesManager,
                "segmentPropertiesManager");
        this.maxNumberOfKeysInIndexPage = maxNumberOfKeysInIndexPage;
        this.segmentDataProvider = Vldtn.requireNonNull(dataProvider,
                "segmentCacheDataProvider");
        this.deltaCacheController = Vldtn.requireNonNull(deltaCacheController,
                "deltaCacheController");
        this.segmentCache = Vldtn.requireNonNull(segmentCache, "segmentCache");
        this.chunkPairFileWriterTx = segmentFiles.getIndexFile().openWriterTx();
        this.scarceIndexWriterTx = segmentFiles.getScarceIndex().openWriterTx();
    }

    @Override
    protected EntryWriter<K, V> doOpen() {
        final EntryWriter<K, Integer> scarceWriter = scarceIndexWriterTx.open();
        segmentFullWriter = new SegmentFullWriter<>(maxNumberOfKeysInIndexPage,
                segmentDataProvider, chunkPairFileWriterTx.openWriter(),
                scarceWriter);
        return segmentFullWriter;
    }

    @Override
    protected void doCommit(final EntryWriter<K, V> writer) {
        scarceIndexWriterTx.commit();
        chunkPairFileWriterTx.commit();
        deltaCacheController.clearPreservingWriteCache();

        segmentPropertiesManager.setNumberOfKeysInCache(0);
        segmentPropertiesManager
                .setNumberOfKeysInIndex(segmentFullWriter.getNumberKeys());
        segmentPropertiesManager.setNumberOfKeysInScarceIndex(
                segmentFullWriter.getNumberKeysInScarceIndex());
    }
}
