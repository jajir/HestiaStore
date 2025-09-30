package org.hestiastore.index.segment;

import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.chunkpairfile.ChunkPairFileWriterTx;
import org.hestiastore.index.scarceindex.ScarceIndexWriterTx;

public class SegmentFullWriterTx<K, V> implements WriteTransaction<K, V> {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final int maxNumberOfKeysInIndexPage;
    private final SegmentDataProvider<K, V> segmentDataProvider;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;
    private final ChunkPairFileWriterTx<K, V> chunkPairFileWriterTx;
    private final ScarceIndexWriterTx<K> scarceIndexWriterTx;
    private SegmentFullWriter<K, V> segmentFullWriter;

    SegmentFullWriterTx(//
            final SegmentFiles<K, V> segmentFiles, //
            final SegmentPropertiesManager propertiesManager, //
            final int maxNumberOfKeysInIndexPage, //
            final SegmentDataProvider<K, V> dataProvider, //
            final SegmentDeltaCacheController<K, V> deltaCacheController//
    ) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
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
    public PairWriter<K, V> openWriter() {
        scarceIndexWriterTx.openWriter();
        segmentFullWriter = new SegmentFullWriter<K, V>(segmentFiles,
                maxNumberOfKeysInIndexPage, segmentDataProvider,
                chunkPairFileWriterTx.openWriter(),
                scarceIndexWriterTx.openWriter());
        return segmentFullWriter;
    }

    @Override
    public void commit() {
        // rename temporal files to main one
        scarceIndexWriterTx.commit();
        chunkPairFileWriterTx.commit();
        deltaCacheController.clear();

        // update segment statistics
        segmentPropertiesManager.setNumberOfKeysInCache(0);
        segmentPropertiesManager
                .setNumberOfKeysInIndex(segmentFullWriter.getNumberKeys());
        segmentPropertiesManager.setNumberOfKeysInScarceIndex(
                segmentFullWriter.getNumberKeysInScarceIndex());
        segmentPropertiesManager.flush();
    }
}
