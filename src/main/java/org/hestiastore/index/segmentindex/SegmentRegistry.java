package org.hestiastore.index.segmentindex;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentBuilder;
import org.hestiastore.index.segment.SegmentConf;
import org.hestiastore.index.segment.SegmentDataSupplier;
import org.hestiastore.index.segment.SegmentFiles;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentPropertiesManager;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResources;
import org.hestiastore.index.segment.SegmentResourcesImpl;

public class SegmentRegistry<K, V> {

    private final Map<SegmentId, Segment<K, V>> segments = new ConcurrentHashMap<>();
    private final Set<SegmentId> splitsInFlight = ConcurrentHashMap.newKeySet();

    private final IndexConfiguration<K, V> conf;
    private final AsyncDirectory directoryFacade;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final SegmentAsyncExecutor segmentAsyncExecutor;
    private final SplitAsyncExecutor splitAsyncExecutor;
    private final ExecutorService maintenanceExecutor;
    private final ExecutorService splitExecutor;

    SegmentRegistry(final AsyncDirectory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.conf = Vldtn.requireNonNull(conf, "conf");
        final Integer maintenanceThreadsConf = conf
                .getNumberOfSegmentIndexMaintenanceThreads();
        final int threads = maintenanceThreadsConf.intValue();
        this.segmentAsyncExecutor = new SegmentAsyncExecutor(threads,
                "segment-maintenance");
        this.maintenanceExecutor = segmentAsyncExecutor.getExecutor();
        final Integer splitThreadsConf = conf
                .getNumberOfIndexMaintenanceThreads();
        final int splitThreads = splitThreadsConf.intValue();
        this.splitAsyncExecutor = new SplitAsyncExecutor(splitThreads,
                "index-maintenance");
        this.splitExecutor = splitAsyncExecutor.getExecutor();
    }

    ExecutorService getMaintenanceExecutor() {
        return maintenanceExecutor;
    }

    ExecutorService getSplitExecutor() {
        return splitExecutor;
    }

    public SegmentResult<Segment<K, V>> getSegment(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        if (splitsInFlight.contains(segmentId)) {
            return SegmentResult.busy();
        }
        final Segment<K, V> out = segments.compute(segmentId,
                (id, existing) -> {
                    if (existing == null || existing.wasClosed()) {
                        return instantiateSegment(id);
                    }
                    return existing;
                });
        return SegmentResult.ok(out);
    }

    void markSplitInFlight(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        splitsInFlight.add(segmentId);
    }

    void clearSplitInFlight(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        splitsInFlight.remove(segmentId);
    }

    boolean isSegmentInstance(final SegmentId segmentId,
            final Segment<K, V> expected) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Vldtn.requireNonNull(expected, "expected");
        return segments.get(segmentId) == expected;
    }

    void executeWithRegistryLock(final Runnable action) {
        Vldtn.requireNonNull(action, "action").run();
    }

    <T> T executeWithRegistryLock(final Supplier<T> action) {
        return Vldtn.requireNonNull(action, "action").get();
    }

    public void removeSegment(final SegmentId segmentId) {
        final Segment<K, V> segment = removeSegmentFromRegistry(segmentId);
        closeSegmentIfNeeded(segment);
        deleteSegmentFiles(segmentId);
    }

    void evictSegment(final SegmentId segmentId) {
        final Segment<K, V> segment = evictSegmentFromRegistry(segmentId);
        closeSegmentIfNeeded(segment);
    }

    /**
     * Evicts a segment only if the registry still points to the provided
     * instance.
     *
     * @param segmentId segment identifier to evict
     * @param expected  expected segment instance bound to the id
     * @return true when the segment was evicted, false otherwise
     */
    boolean evictSegmentIfSame(final SegmentId segmentId,
            final Segment<K, V> expected) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Vldtn.requireNonNull(expected, "expected");
        if (!segments.remove(segmentId, expected)) {
            return false;
        }
        closeSegmentIfNeeded(expected);
        return true;
    }

    protected Segment<K, V> removeSegmentFromRegistry(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        return segments.remove(segmentId);
    }

    protected Segment<K, V> evictSegmentFromRegistry(
            final SegmentId segmentId) {
        return removeSegmentFromRegistry(segmentId);
    }

    SegmentBuilder<K, V> newSegmentBuilder(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final SegmentConf segmentConf = buildSegmentConf();
        final SegmentPropertiesManager segmentPropertiesManager = newSegmentPropertiesManager(
                segmentId);
        final SegmentFiles<K, V> segmentFiles = newSegmentFiles(segmentId);
        final SegmentDataSupplier<K, V> segmentDataSupplier = new SegmentDataSupplier<>(
                segmentFiles, segmentConf, segmentPropertiesManager);
        final SegmentResources<K, V> dataProvider = new SegmentResourcesImpl<>(
                segmentDataSupplier);

        return Segment.<K, V>builder()//
                .withAsyncDirectory(directoryFacade)//
                .withId(segmentId)//
                .withKeyTypeDescriptor(keyTypeDescriptor)//
                .withSegmentResources(dataProvider)//
                .withSegmentConf(segmentConf)//
                .withMaintenanceExecutor(maintenanceExecutor)//
                .withSegmentMaintenanceAutoEnabled(Boolean.TRUE
                        .equals(conf.isSegmentMaintenanceAutoEnabled()))//
                .withSegmentFiles(segmentFiles)//
                .withSegmentPropertiesManager(segmentPropertiesManager)//
                .withMaxNumberOfKeysInSegmentWriteCache(
                        conf.getMaxNumberOfKeysInSegmentWriteCache().intValue())//
                .withMaxNumberOfKeysInSegmentCache(
                        conf.getMaxNumberOfKeysInSegmentCache())//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringFlush(
                        conf.getMaxNumberOfKeysInSegmentWriteCacheDuringFlush()
                                .intValue())//
                .withMaxNumberOfKeysInSegmentChunk(
                        conf.getMaxNumberOfKeysInSegmentChunk())//
                .withValueTypeDescriptor(valueTypeDescriptor)//
                .withBloomFilterNumberOfHashFunctions(
                        conf.getBloomFilterNumberOfHashFunctions())//
                .withBloomFilterIndexSizeInBytes(
                        conf.getBloomFilterIndexSizeInBytes())//
                .withDiskIoBufferSize(conf.getDiskIoBufferSize());
    }

    SegmentFiles<K, V> newSegmentFiles(final SegmentId segmentId) {
        return new SegmentFiles<>(directoryFacade, segmentId, keyTypeDescriptor,
                valueTypeDescriptor, conf.getDiskIoBufferSize(),
                conf.getEncodingChunkFilters(), conf.getDecodingChunkFilters());
    }

    SegmentPropertiesManager newSegmentPropertiesManager(
            final SegmentId segmentId) {
        return new SegmentPropertiesManager(directoryFacade, segmentId);
    }

    private SegmentConf buildSegmentConf() {
        return new SegmentConf(
                conf.getMaxNumberOfKeysInSegmentWriteCache().intValue(),
                conf.getMaxNumberOfKeysInSegmentWriteCacheDuringFlush()
                        .intValue(),
                conf.getMaxNumberOfKeysInSegmentCache(),
                conf.getMaxNumberOfKeysInSegmentChunk(),
                conf.getBloomFilterNumberOfHashFunctions(),
                conf.getBloomFilterIndexSizeInBytes(),
                conf.getBloomFilterProbabilityOfFalsePositive(),
                conf.getDiskIoBufferSize(), conf.getEncodingChunkFilters(),
                conf.getDecodingChunkFilters());
    }

    private Segment<K, V> instantiateSegment(final SegmentId segmentId) {
        return newSegmentBuilder(segmentId).build();
    }

    protected void deleteSegmentFiles(final SegmentId segmentId) {
        final SegmentFiles<K, V> segmentFiles = newSegmentFiles(segmentId);
        final SegmentPropertiesManager segmentPropertiesManager = newSegmentPropertiesManager(
                segmentId);
        segmentFiles.deleteAllFiles(segmentPropertiesManager);
    }

    private void closeSegmentIfNeeded(final Segment<K, V> segment) {
        if (segment != null && !segment.wasClosed()) {
            segment.close();
        }
    }

    public void close() {
        if (!segmentAsyncExecutor.wasClosed()) {
            segmentAsyncExecutor.close();
        }
        if (!splitAsyncExecutor.wasClosed()) {
            splitAsyncExecutor.close();
        }
    }

}
