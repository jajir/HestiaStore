package com.coroptis.index.segment;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.OptimisticLockObjectVersionProvider;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairWriter;
import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.scarceindex.ScarceIndex;

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
    private final SegmentPropertiesController segmentStatsController;
    private final SegmentCompacter<K, V> segmentCompacter;

    public static <M, N> SegmentBuilder<M, N> builder() {
        return new SegmentBuilder<>();
    }

    public Segment(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final VersionController versionController) {
        this.segmentConf = Objects.requireNonNull(segmentConf);
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        logger.debug("Initializing segment '{}'", segmentFiles.getId());
        this.versionController = Objects.requireNonNull(versionController,
                "Version controller is required");
        this.segmentStatsController = new SegmentPropertiesController(
                segmentFiles.getDirectory(), segmentFiles.getId(),
                versionController);
        this.segmentCompacter = new SegmentCompacter<>(segmentFiles,
                segmentConf, versionController);
    }

    private ScarceIndex<K> getScarceIndex() {
        return ScarceIndex.<K>builder()
                .withDirectory(segmentFiles.getDirectory())
                .withFileName(segmentFiles.getScarceFileName())
                .withKeyTypeDescriptor(segmentFiles.getKeyTypeDescriptor())
                .build();
    }

    // FIXME remove it
    @Deprecated
    public K getMaxKey() {
        return getScarceIndex().getMaxKey();
    }

    // FIXME remove it
    @Deprecated
    public K getMinKey() {
        return getScarceIndex().getMinKey();
    }

    public SegmentStats getStats() {
        return segmentStatsController.getSegmentStatsManager()
                .getSegmentStats();
    }

    public void optionallyCompact() {
        segmentCompacter.optionallyCompact();
    }

    public PairIterator<K, V> openIterator() {
        // TODO this naive implementation ignores possible in memory cache.
        return new SegmentReader<>(segmentFiles)
                .openIterator(versionController);
    }

    public void forceCompact() {
        segmentCompacter.forceCompact();
    }

    /**
     * Method should be called just from inside of this package. Method open
     * direct writer to scarce index and main sst file. It's useful for
     * compacting.
     */
    SegmentFullWriter<K, V> openFullWriter() {
        final BloomFilter<K> bloomFilter = BloomFilter.<K>builder()
                .withBloomFilterFileName(segmentFiles.getBloomFilterFileName())
                .withConvertorToBytes(segmentFiles.getKeyTypeDescriptor()
                        .getConvertorToBytes())
                .withDirectory(segmentFiles.getDirectory())
                .withIndexSizeInBytes(
                        segmentConf.getBloomFilterIndexSizeInBytes())
                .withNumberOfHashFunctions(
                        segmentConf.getBloomFilterNumberOfHashFunctions())
                .build();
        return new SegmentFullWriter<K, V>(bloomFilter, segmentFiles,
                segmentStatsController,
                segmentConf.getMaxNumberOfKeysInIndexPage());
    }

    public PairWriter<K, V> openWriter() {
        final SegmentWriter<K, V> writer = new SegmentWriter<>(segmentFiles,
                segmentFiles.getKeyTypeDescriptor(),
                segmentStatsController.getSegmentStatsManager(),
                versionController, segmentCompacter);
        return writer.openWriter();
    }

    public SegmentSearcher<K, V> openSearcher() {
        return new SegmentSearcher<>(segmentFiles, segmentConf,
                versionController);
    }

    public Segment<K, V> split(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId);
        versionController.changeVersion();

        final SegmentSplitter<K, V> segmentSplitter = new SegmentSplitter<>(
                segmentFiles, segmentConf, versionController);
        return segmentSplitter.split(segmentId);
    }

    @Override
    public void close() {
//        bloomFilter.logStats();
        logger.debug("Closing segment '{}'", segmentFiles.getId());
        // Do intentionally nothing.
    }

    public SegmentId getId() {
        return segmentFiles.getId();
    }

    @Override
    public int getVersion() {
        return versionController.getVersion();
    }

}
