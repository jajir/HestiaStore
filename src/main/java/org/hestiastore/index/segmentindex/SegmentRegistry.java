package org.hestiastore.index.segmentindex;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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

    private final LinkedHashMap<SegmentId, Segment<K, V>> segments = new LinkedHashMap<>(
            16, 0.75f, true);
    private final Object segmentsLock = new Object();
    private final Set<SegmentId> splitsInFlight = ConcurrentHashMap.newKeySet();

    private final IndexConfiguration<K, V> conf;
    private final AsyncDirectory directoryFacade;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final SegmentAsyncExecutor segmentAsyncExecutor;
    private final SplitAsyncExecutor splitAsyncExecutor;
    private final ExecutorService maintenanceExecutor;
    private final ExecutorService splitExecutor;
    private final int maxNumberOfSegmentsInCache;
    private final boolean segmentRootDirectoryEnabled;
    private final SegmentDirectorySwap directorySwap;

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
        final int maxSegments = Vldtn
                .requireNonNull(conf.getMaxNumberOfSegmentsInCache(),
                        "maxNumberOfSegmentsInCache")
                .intValue();
        this.maxNumberOfSegmentsInCache = Vldtn.requireGreaterThanZero(
                maxSegments, "maxNumberOfSegmentsInCache");
        this.segmentRootDirectoryEnabled = Boolean.TRUE
                .equals(conf.isSegmentRootDirectoryEnabled());
        this.directorySwap = segmentRootDirectoryEnabled
                ? new SegmentDirectorySwap(directoryFacade)
                : null;
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
        final List<Segment<K, V>> evicted = new ArrayList<>();
        final Segment<K, V> out;
        synchronized (segmentsLock) {
            out = getOrCreateSegmentLocked(segmentId);
            evictIfNeededLocked(evicted);
        }
        closeEvictedSegments(evicted);
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
        synchronized (segmentsLock) {
            return segments.get(segmentId) == expected;
        }
    }

    void executeWithRegistryLock(final Runnable action) {
        Vldtn.requireNonNull(action, "action");
        synchronized (segmentsLock) {
            action.run();
        }
    }

    <T> T executeWithRegistryLock(final Supplier<T> action) {
        Vldtn.requireNonNull(action, "action");
        synchronized (segmentsLock) {
            return action.get();
        }
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
        final Segment<K, V> removed;
        synchronized (segmentsLock) {
            final Segment<K, V> current = segments.get(segmentId);
            if (current != expected) {
                return false;
            }
            removed = segments.remove(segmentId);
        }
        closeSegmentIfNeeded(removed);
        return removed != null;
    }

    protected Segment<K, V> removeSegmentFromRegistry(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        synchronized (segmentsLock) {
            return segments.remove(segmentId);
        }
    }

    protected Segment<K, V> evictSegmentFromRegistry(
            final SegmentId segmentId) {
        return removeSegmentFromRegistry(segmentId);
    }

    SegmentBuilder<K, V> newSegmentBuilder(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final SegmentConf segmentConf = buildSegmentConf();
        final SegmentBuilder<K, V> builder = Segment.<K, V>builder()//
                .withAsyncDirectory(directoryFacade)//
                .withId(segmentId)//
                .withKeyTypeDescriptor(keyTypeDescriptor)//
                .withSegmentConf(segmentConf)//
                .withMaintenanceExecutor(maintenanceExecutor)//
                .withSegmentMaintenanceAutoEnabled(Boolean.TRUE
                        .equals(conf.isSegmentMaintenanceAutoEnabled()))//
                .withSegmentRootDirectoryEnabled(segmentRootDirectoryEnabled)//
                .withMaxNumberOfKeysInSegmentWriteCache(
                        conf.getMaxNumberOfKeysInSegmentWriteCache().intValue())//
                .withMaxNumberOfKeysInSegmentCache(
                        conf.getMaxNumberOfKeysInSegmentCache())//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(
                        conf.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance()
                                .intValue())//
                .withMaxNumberOfKeysInSegmentChunk(
                        conf.getMaxNumberOfKeysInSegmentChunk())//
                .withValueTypeDescriptor(valueTypeDescriptor)//
                .withBloomFilterNumberOfHashFunctions(
                        conf.getBloomFilterNumberOfHashFunctions())//
                .withBloomFilterIndexSizeInBytes(
                        conf.getBloomFilterIndexSizeInBytes())//
                .withDiskIoBufferSize(conf.getDiskIoBufferSize());
        if (!segmentRootDirectoryEnabled) {
            final SegmentPropertiesManager segmentPropertiesManager = newSegmentPropertiesManager(
                    segmentId);
            final SegmentFiles<K, V> segmentFiles = newSegmentFiles(segmentId);
            final SegmentDataSupplier<K, V> segmentDataSupplier = new SegmentDataSupplier<>(
                    segmentFiles, segmentConf);
            final SegmentResources<K, V> dataProvider = new SegmentResourcesImpl<>(
                    segmentDataSupplier);
            builder.withSegmentResources(dataProvider)//
                    .withSegmentFiles(segmentFiles)//
                    .withSegmentPropertiesManager(segmentPropertiesManager);
        }
        return builder;
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
                conf.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance()
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
        if (segmentRootDirectoryEnabled) {
            deleteSegmentRootDirectory(segmentId);
            return;
        }
        final SegmentFiles<K, V> segmentFiles = newSegmentFiles(segmentId);
        final SegmentPropertiesManager segmentPropertiesManager = newSegmentPropertiesManager(
                segmentId);
        segmentFiles.deleteAllFiles(segmentPropertiesManager);
    }

    boolean isSegmentRootDirectoryEnabled() {
        return segmentRootDirectoryEnabled;
    }

    void swapSegmentDirectories(final SegmentId segmentId,
            final SegmentId replacementSegmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Vldtn.requireNonNull(replacementSegmentId, "replacementSegmentId");
        if (!segmentRootDirectoryEnabled) {
            throw new IllegalStateException(
                    "Segment root directory layout is disabled.");
        }
        directorySwap.swap(segmentId, replacementSegmentId);
    }

    void recoverDirectorySwaps(final List<SegmentId> segmentIds) {
        Vldtn.requireNonNull(segmentIds, "segmentIds");
        if (!segmentRootDirectoryEnabled) {
            return;
        }
        for (final SegmentId segmentId : segmentIds) {
            directorySwap.recoverIfNeeded(segmentId);
        }
    }

    private void deleteSegmentRootDirectory(final SegmentId segmentId) {
        if (directorySwap == null) {
            return;
        }
        directorySwap.deleteSegmentRootDirectory(segmentId);
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

    private Segment<K, V> getOrCreateSegmentLocked(final SegmentId segmentId) {
        Segment<K, V> existing = segments.get(segmentId);
        if (existing == null || existing.wasClosed()) {
            existing = instantiateSegment(segmentId);
            segments.put(segmentId, existing);
        }
        return existing;
    }

    private void evictIfNeededLocked(final List<Segment<K, V>> evicted) {
        while (segments.size() > maxNumberOfSegmentsInCache) {
            final Map.Entry<SegmentId, Segment<K, V>> eldest = segments
                    .entrySet().iterator().next();
            segments.remove(eldest.getKey());
            evicted.add(eldest.getValue());
        }
    }

    private void closeEvictedSegments(final List<Segment<K, V>> evicted) {
        if (evicted.isEmpty()) {
            return;
        }
        for (final Segment<K, V> segment : evicted) {
            closeSegmentIfNeeded(segment);
        }
    }

}
