package org.hestiastore.index.segment;

import java.util.Objects;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.OptimisticLock;
import org.hestiastore.index.OptimisticLockObjectVersionProvider;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.PairIteratorWithLock;
import org.hestiastore.index.PairWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
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
    private final SegmentManager<K, V> segmentManager;

    public static <M, N> SegmentBuilder<M, N> builder() {
        return new SegmentBuilder<>();
    }

    public Segment(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final VersionController versionController,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentDataProvider<K, V> segmentDataProvider,
            final SegmentSearcher<K, V> segmentSearcher,
            final SegmentManager<K, V> segmentManager) {
        this.segmentConf = Objects.requireNonNull(segmentConf);
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        logger.debug("Initializing segment '{}'", segmentFiles.getId());
        this.versionController = Objects.requireNonNull(versionController,
                "Version controller is required");
        Objects.requireNonNull(segmentDataProvider,
                "Segment cached data provider is required");
        this.segmentPropertiesManager = Objects.requireNonNull(
                segmentPropertiesManager,
                "Segment properties manager is required");
        deltaCacheController = new SegmentDeltaCacheController<>(segmentFiles,
                segmentPropertiesManager, segmentDataProvider);
        this.segmentCompacter = new SegmentCompacter<>(this, segmentFiles,
                segmentConf, versionController, segmentPropertiesManager);
        this.segmentSearcher = Objects.requireNonNull(segmentSearcher);
        this.segmentManager = Objects.requireNonNull(segmentManager,
                "Segment manager is required");
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
                segmentFiles.getIndexSstFileForIteration().openIterator(),
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
    SegmentFullWriter<K, V> openFullWriter() {
        return segmentManager.createSegmentFullWriter();
    }

    public PairWriter<K, V> openWriter() {
        versionController.changeVersion();
        return new SegmentWriter<>(segmentCompacter, deltaCacheController);
    }

    public V get(final K key) {
        return segmentSearcher.get(key);
    }

    /**
     * Provide class that helps with segment splitting into two. It should be
     * used when segment is too big.
     * 
     * @return
     */
    public SegmentSplitter<K, V> getSegmentSplitter() {
        return new SegmentSplitter<>(this, segmentFiles, versionController,
                segmentPropertiesManager, deltaCacheController, segmentManager);
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
