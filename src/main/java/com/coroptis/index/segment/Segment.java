package com.coroptis.index.segment;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.OptimisticLockObjectVersionProvider;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairWriter;

/**
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class Segment<K, V>
        implements CloseableResource, OptimisticLockObjectVersionProvider {

    private final Logger logger = LoggerFactory.getLogger(Segment.class);
    private final SegmentConf segmentConf;
    private final SegmentFiles<K, V> segmentFiles;
    private final VersionController versionController;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentCompacter<K, V> segmentCompacter;
    private final SegmentCacheDataProvider<K, V> segmentCacheDataProvider;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;

    public static <M, N> SegmentBuilder<M, N> builder() {
        return new SegmentBuilder<>();
    }

    public Segment(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final VersionController versionController,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentCacheDataProvider<K, V> segmentCacheDataProvider) {
        this.segmentConf = Objects.requireNonNull(segmentConf);
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        logger.debug("Initializing segment '{}'", segmentFiles.getId());
        this.versionController = Objects.requireNonNull(versionController,
                "Version controller is required");
        this.segmentCacheDataProvider = Objects.requireNonNull(
                segmentCacheDataProvider,
                "Segment cached data provider is required");
        this.segmentPropertiesManager = Objects.requireNonNull(
                segmentPropertiesManager,
                "Segment properties manager is required");
        deltaCacheController = new SegmentDeltaCacheController<>(
                segmentFiles.getKeyTypeDescriptor(), segmentFiles,
                segmentPropertiesManager, segmentCacheDataProvider);
        this.segmentCompacter = new SegmentCompacter<>(segmentFiles,
                segmentConf, versionController, segmentPropertiesManager,
                segmentCacheDataProvider, deltaCacheController);
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

    public PairIterator<K, V> openIterator() {
        return new SegmentReader<>(segmentFiles, deltaCacheController)
                .openIterator(versionController);
    }

    public void forceCompact() {
        if (segmentPropertiesManager.getCacheDeltaFileNames().size() > 0) {
            segmentCompacter.forceCompact();
        }
    }

    /**
     * Method should be called just from inside of this package. Method open
     * direct writer to scarce index and main sst file. It's useful for
     * compacting.
     */
    SegmentFullWriter<K, V> openFullWriter() {
        return new SegmentFullWriter<K, V>(segmentFiles,
                segmentPropertiesManager,
                segmentConf.getMaxNumberOfKeysInIndexPage(),
                segmentCacheDataProvider, deltaCacheController);
    }

    public PairWriter<K, V> openWriter() {
        return new SegmentWriter<>(segmentCompacter, deltaCacheController);
    }

    public SegmentSearcher<K, V> openSearcher() {
        final SegmentIndexSearcherSupplier<K, V> supplier = new SegmentIndexSearcherDefaultSupplier<>(
                segmentFiles, segmentConf);
        return new SegmentSearcher<>(segmentFiles, segmentConf,
                versionController, segmentPropertiesManager, supplier,
                segmentCacheDataProvider);
    }

    /**
     * Provide object that loads data objects. Shouldn't be used directly withou
     * caching.
     * 
     * @return
     */
    public SegmentCacheDataProvider<K, V> getCacheDataProvider() {
        return new SegmentCacheDataDirectLoader<>(segmentFiles, segmentConf,
                segmentPropertiesManager);
    }

    public SegmentSplitter.Result<K, V> split(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId);
        final SegmentSplitter<K, V> segmentSplitter = new SegmentSplitter<>(
                segmentFiles, segmentConf, versionController,
                segmentPropertiesManager, segmentCacheDataProvider,
                deltaCacheController);
        return segmentSplitter.split(segmentId);
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

}
