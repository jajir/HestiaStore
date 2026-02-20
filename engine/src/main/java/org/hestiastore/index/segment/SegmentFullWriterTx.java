package org.hestiastore.index.segment;

import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.GuardedWriteTransaction;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.chunkentryfile.ChunkEntryFileWriterTx;
import org.hestiastore.index.scarceindex.ScarceIndexWriterTx;

/**
 * Transaction that rebuilds the full segment index and metadata.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class SegmentFullWriterTx<K, V>
        extends GuardedWriteTransaction<EntryWriter<K, V>>
        implements WriteTransaction<K, V> {

    private final SegmentPropertiesManager segmentPropertiesManager;
    private final int maxNumberOfKeysInIndexPage;
    private final SegmentResources<K, V> segmentDataProvider;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;
    private final ChunkEntryFileWriterTx<K, V> chunkPairFileWriterTx;
    private final ScarceIndexWriterTx<K> scarceIndexWriterTx;
    private SegmentFullWriter<K, V> segmentFullWriter;

    /**
     * Creates a full writer transaction for the given segment files.
     *
     * @param segmentFiles segment file access wrapper
     * @param propertiesManager properties manager for stats updates
     * @param maxNumberOfKeysInIndexPage keys per index page
     * @param dataProvider segment resources provider
     * @param deltaCacheController delta cache controller
     */
    SegmentFullWriterTx(final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager propertiesManager,
            final int maxNumberOfKeysInIndexPage,
            final SegmentResources<K, V> dataProvider,
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

    /**
     * Opens the writer used to stream entries into the rebuilt segment.
     *
     * @return entry writer for the transaction
     */
    @Override
    protected EntryWriter<K, V> doOpen() {
        final EntryWriter<K, Integer> scarceWriter = scarceIndexWriterTx.open();
        segmentFullWriter = new SegmentFullWriter<>(maxNumberOfKeysInIndexPage,
                segmentDataProvider, chunkPairFileWriterTx.openWriter(),
                scarceWriter);
        return segmentFullWriter;
    }

    /**
     * Commits the rebuilt segment files and updates metadata.
     *
     * @param writer entry writer used during the transaction
     */
    @Override
    protected void doCommit(final EntryWriter<K, V> writer) {
        scarceIndexWriterTx.commit();
        chunkPairFileWriterTx.commit();
        segmentFullWriter.commitBloomFilter();
        deltaCacheController.clearPreservingWriteCache();

        segmentPropertiesManager.setKeyCounters(0,
                segmentFullWriter.getNumberKeys(),
                segmentFullWriter.getNumberKeysInScarceIndex());
    }
}
