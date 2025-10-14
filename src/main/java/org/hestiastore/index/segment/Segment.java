package org.hestiastore.index.segment;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.OptimisticLock;
import org.hestiastore.index.OptimisticLockObjectVersionProvider;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.PairIteratorWithLock;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.WriteTransaction.WriterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single on-disk index segment with delta-cache and compaction support.
 * <p>
 * Segment coordinates read and write operations for a bounded subset of the
 * index data. It encapsulates the underlying files, provides search and
 * iteration, accepts writes through a delta cache (with optional automatic
 * compaction), and exposes utilities for statistics, consistency checking, and
 * splitting oversized segments. Versioning is tracked via an optimistic lock to
 * guard concurrent readers while updates occur.
 *
 * @author honza
 *
 * @param <K> key type stored in this segment
 * @param <V> value type stored in this segment
 */
public class Segment<K, V>
        implements CloseableResource, OptimisticLockObjectVersionProvider {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final SegmentConf segmentConf;
    private final SegmentFiles<K, V> segmentFiles;
    private final VersionController versionController;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentCompacter<K, V> segmentCompacter;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;
    private final SegmentSearcher<K, V> segmentSearcher;
    private final SegmentDataProvider<K, V> segmentCacheDataProvider;

    public static <M, N> SegmentBuilder<M, N> builder() {
        return new SegmentBuilder<>();
    }

    public Segment(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final VersionController versionController,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentDataProvider<K, V> segmentDataProvider,
            final SegmentSearcher<K, V> segmentSearcher,
            final SegmentDataProvider<K, V> segmentCacheDataProvider) {
        this.segmentConf = Vldtn.requireNonNull(segmentConf, "segmentConf");
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        logger.debug("Initializing segment '{}'", segmentFiles.getId());
        this.versionController = Vldtn.requireNonNull(versionController,
                "versionController");
        Vldtn.requireNonNull(segmentDataProvider, "segmentDataProvider");
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
        deltaCacheController = new SegmentDeltaCacheController<>(segmentFiles,
                segmentPropertiesManager, segmentDataProvider);
        this.segmentCompacter = new SegmentCompacter<>(this, segmentFiles,
                segmentConf, versionController, segmentPropertiesManager);
        this.segmentSearcher = Vldtn.requireNonNull(segmentSearcher,
                "segmentSearcher");
        this.segmentCacheDataProvider = Vldtn.requireNonNull(
                segmentCacheDataProvider, "segmentCacheDataProvider");
    }

    public SegmentStats getStats() {
        return segmentPropertiesManager.getSegmentStats();
    }

    public long getNumberOfKeys() {
        return segmentPropertiesManager.getSegmentStats().getNumberOfKeys();
    }

    public void optionallyCompact() {
        segmentCompacter.optionallyCompact();
    }

    public K checkAndRepairConsistency() {
        final SegmentConsistencyChecker<K, V> consistencyChecker = new SegmentConsistencyChecker<>(
                this, segmentFiles.getKeyTypeDescriptor().getComparator());
        return consistencyChecker.checkAndRepairConsistency();
    }

    public PairIterator<K, V> openIterator() {
        final PairIterator<K, V> mergedPairIterator = new MergeDeltaCacheWithIndexIterator<>(
                segmentFiles.getIndexFile().openIterator(),
                segmentFiles.getKeyTypeDescriptor(),
                segmentFiles.getValueTypeDescriptor(),
                deltaCacheController.getDeltaCache().getAsSortedList());
        return new PairIteratorWithLock<>(mergedPairIterator,
                new OptimisticLock(versionController), getId().toString());
    }

    public void forceCompact() {
        if (!segmentPropertiesManager.getCacheDeltaFileNames().isEmpty()) {
            segmentCompacter.forceCompact();
        }
    }

    /**
     * Method should be called just from inside of this package. Method open
     * direct writer to scarce index and main sst file.
     * 
     * Writer should be opend and closed as one atomic operation.
     * 
     * @return return segment writer object
     */
    void executeFullWriteTx(final WriterFunction<K, V> writeFunction) {
        openFullWriteTx().execute(writeFunction);
    }

    WriteTransaction<K, V> openFullWriteTx() {
        return new SegmentFullWriterTx<>(segmentFiles, segmentPropertiesManager,
                segmentConf.getMaxNumberOfKeysInChunk(),
                segmentCacheDataProvider, deltaCacheController);
    }

    /**
     * Allows to open writer that will write to delta cache. When number of keys
     * in segment exceeds certain threshold, delta cache will be flushed to
     * disk.
     * 
     * It's not necesarry to run it in transaction because it's always new file.
     */
    public PairWriter<K, V> openDeltaCacheWriter() {
        versionController.changeVersion();
        return new SegmentDeltaCacheCompactingWriter<>(segmentCompacter,
                deltaCacheController);
    }

    public V get(final K key) {
        return segmentSearcher.get(key);
    }

    /**
     * Create new segment.
     * 
     * @param segmentId rqeuired segment id
     * @return initialized segment
     */
    public Segment<K, V> createSegment(SegmentId segmentId) {
        return Segment.<K, V>builder()
                .withDirectory(segmentFiles.getDirectory())//
                .withId(segmentId)//
                .withKeyTypeDescriptor(segmentFiles.getKeyTypeDescriptor())//
                .withValueTypeDescriptor(segmentFiles.getValueTypeDescriptor())//
                .withSegmentConf(new SegmentConf(segmentConf))//
                .build();
    }

    /**
     * Provide class that helps with segment splitting into two. It should be
     * used when segment is too big.
     * 
     * @return
     */
    public SegmentSplitter<K, V> getSegmentSplitter() {
        final SegmentFilesRenamer segmentFilesRenamer = new SegmentFilesRenamer();
        return new SegmentSplitter<>(this, segmentFiles, versionController,
                segmentPropertiesManager, deltaCacheController,
                segmentFilesRenamer);
    }

    @Override
    public void close() {
        logger.debug("Closing segment '{}'", segmentFiles.getId());
    }

    public SegmentId getId() {
        return segmentFiles.getId();
    }

    @Override
    public int getVersion() {
        return versionController.getVersion();
    }

    public SegmentFiles<K, V> getSegmentFiles() {
        return segmentFiles;
    }

    public SegmentConf getSegmentConf() {
        return segmentConf;
    }

    public SegmentPropertiesManager getSegmentPropertiesManager() {
        return segmentPropertiesManager;
    }

}
