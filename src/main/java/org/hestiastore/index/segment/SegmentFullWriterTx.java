package org.hestiastore.index.segment;

import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.chunkpairfile.ChunkPairFileWriterTx;

public class SegmentFullWriterTx<K, V> implements WriteTransaction<K, V> {

    private final SegmentManager<K, V> segmentManager;
    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final int maxNumberOfKeysInIndexPage;
    private final SegmentDataProvider<K, V> segmentDataProvider;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;
    private final ChunkPairFileWriterTx<K, V> chunkPairFileWriterTx;
    private SegmentFullWriter<K, V> segmentFullWriter;

    SegmentFullWriterTx(//
            final SegmentManager<K, V> segmentManager, //
            final SegmentFiles<K, V> segmentFiles, //
            final SegmentPropertiesManager propertiesManager, //
            final int maxNumberOfKeysInIndexPage, //
            final SegmentDataProvider<K, V> dataProvider, //
            final SegmentDeltaCacheController<K, V> deltaCacheController//
    ) {
        this.segmentManager = Vldtn.requireNonNull(segmentManager,
                "segmentManager");
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.segmentPropertiesManager = Vldtn.requireNonNull(propertiesManager,
                "segmentPropertiesManager");
        this.maxNumberOfKeysInIndexPage = maxNumberOfKeysInIndexPage;
        this.segmentDataProvider = Vldtn.requireNonNull(dataProvider,
                "segmentCacheDataProvider");
        this.deltaCacheController = Vldtn.requireNonNull(deltaCacheController,
                "deltaCacheController");
        this.chunkPairFileWriterTx = segmentFiles.getIndexFile().openWriterTx();
    }

    @Override
    public PairWriter<K, V> openWriter() {
        segmentFullWriter = new SegmentFullWriter<K, V>(segmentFiles,
                maxNumberOfKeysInIndexPage, segmentDataProvider,
                chunkPairFileWriterTx.openWriter());
        return segmentFullWriter;
    }

    @Override
    public void commit() {
        // rename temporal files to main one
        segmentFiles.getDirectory().renameFile(
                segmentFiles.getTempScarceFileName(),
                segmentFiles.getScarceFileName());

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
