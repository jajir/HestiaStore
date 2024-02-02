package com.coroptis.index.segment;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.OptimisticLockObjectVersionProvider;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairWriter;
import com.coroptis.index.bloomfilter.BloomFilter;

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
        this.segmentPropertiesManager = new SegmentPropertiesManager(
                segmentFiles.getDirectory(), getId());
        this.segmentCompacter = new SegmentCompacter<>(segmentFiles,
                segmentConf, versionController, segmentPropertiesManager);
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
        return openIterator(null);
    }

    public PairIterator<K, V> openIterator(
            final SegmentSearcher<K, V> segmentSearcher) {
        return new SegmentReader<>(segmentFiles, segmentPropertiesManager)
                .openIterator(versionController, segmentSearcher);
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
                segmentPropertiesManager,
                segmentConf.getMaxNumberOfKeysInIndexPage());
    }

    public PairWriter<K, V> openWriter() {
        return openWriter(null);
    }

    public PairWriter<K, V> openWriter(
            final SegmentSearcher<K, V> segmentSearcher) {
        final SegmentWriter<K, V> writer = new SegmentWriter<>(segmentFiles,
                segmentPropertiesManager, segmentCompacter);
        return writer.openWriter(segmentSearcher);
    }

    public SegmentSearcher<K, V> openSearcher() {
        return new SegmentSearcher<>(segmentFiles, segmentConf,
                versionController, segmentPropertiesManager);
    }

    public SegmentSplitter.Result<K, V> split(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId);
        final SegmentSplitter<K, V> segmentSplitter = new SegmentSplitter<>(
                segmentFiles, segmentConf, versionController,
                segmentPropertiesManager);
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
