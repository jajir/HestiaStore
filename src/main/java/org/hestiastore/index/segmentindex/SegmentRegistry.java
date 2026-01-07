package org.hestiastore.index.segmentindex;

import java.util.HashMap;
import java.util.Map;
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
import org.hestiastore.index.segment.SegmentResources;
import org.hestiastore.index.segment.SegmentResourcesImpl;
import org.hestiastore.index.segmentasync.SegmentAsyncAdapter;
import org.hestiastore.index.segmentasync.SegmentAsyncExecutor;
import org.hestiastore.index.segmentasync.SegmentMaintenancePolicy;
import org.hestiastore.index.segmentasync.SegmentMaintenancePolicyThreshold;

public class SegmentRegistry<K, V> {

    private final Map<SegmentId, Segment<K, V>> segments = new HashMap<>();

    private final IndexConfiguration<K, V> conf;
    private final AsyncDirectory directoryFacade;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final SegmentAsyncExecutor segmentAsyncExecutor;
    private final ExecutorService maintenanceExecutor;
    private final boolean ownsExecutor;
    private final SegmentMaintenancePolicy<K, V> maintenancePolicy;

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
        final Integer threadsConf = conf.getNumberOfThreads();
        final int threads = (threadsConf == null || threadsConf < 1) ? 1
                : threadsConf.intValue();
        final ExecutorService providedExecutor = conf
                .getMaintenanceExecutor();
        if (providedExecutor == null) {
            this.segmentAsyncExecutor = new SegmentAsyncExecutor(threads,
                    "segment-async");
            this.maintenanceExecutor = segmentAsyncExecutor.getExecutor();
            this.ownsExecutor = true;
        } else {
            this.segmentAsyncExecutor = null;
            this.maintenanceExecutor = providedExecutor;
            this.ownsExecutor = false;
        }
        this.maintenancePolicy = new SegmentMaintenancePolicyThreshold<>(
                conf.getMaxNumberOfKeysInSegmentWriteCache(),
                conf.getMaxNumberOfKeysInSegmentCache());
    }

    public Segment<K, V> getSegment(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Segment<K, V> out = segments.get(segmentId);
        if (out == null || out.wasClosed()) {
            out = instantiateSegment(segmentId);
            segments.put(segmentId, out);
        }
        return out;
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
        final Segment<K, V> current = segments.get(segmentId);
        if (current != expected) {
            return false;
        }
        segments.remove(segmentId);
        closeSegmentIfNeeded(current);
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
        final Segment<K, V> segment = newSegmentBuilder(segmentId).build();
        return new SegmentAsyncAdapter<>(segment, maintenanceExecutor,
                maintenancePolicy);
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
        if (ownsExecutor && segmentAsyncExecutor != null
                && !segmentAsyncExecutor.wasClosed()) {
            segmentAsyncExecutor.close();
        }
    }

}
