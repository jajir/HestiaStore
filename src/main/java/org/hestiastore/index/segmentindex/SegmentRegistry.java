package org.hestiastore.index.segmentindex;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentBuilder;
import org.hestiastore.index.segment.SegmentFiles;
import org.hestiastore.index.segment.SegmentFilesRenamer;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentPropertiesManager;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentRegistry<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
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
    private final SegmentDirectorySwap directorySwap;
    private final SegmentFilesRenamer filesRenamer = new SegmentFilesRenamer();
    private final IndexRetryPolicy retryPolicy;

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
        this.directorySwap = new SegmentDirectorySwap(directoryFacade);
        final int busyBackoffMillis = sanitizeRetryConf(
                conf.getIndexBusyBackoffMillis(),
                IndexConfigurationContract.DEFAULT_INDEX_BUSY_BACKOFF_MILLIS);
        final int busyTimeoutMillis = sanitizeRetryConf(
                conf.getIndexBusyTimeoutMillis(),
                IndexConfigurationContract.DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS);
        this.retryPolicy = new IndexRetryPolicy(busyBackoffMillis,
                busyTimeoutMillis);
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
        final boolean lockedBusy;
        synchronized (segmentsLock) {
            Segment<K, V> created = null;
            boolean busy = false;
            try {
                created = getOrCreateSegmentLocked(segmentId);
            } catch (final IllegalStateException e) {
                if (isSegmentLockConflict(e)) {
                    busy = true;
                } else {
                    throw e;
                }
            }
            lockedBusy = busy;
            out = created;
            if (!lockedBusy) {
                evictIfNeededLocked(evicted);
            }
        }
        if (lockedBusy) {
            return SegmentResult.busy();
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
        final AsyncDirectory segmentDirectory = openSegmentDirectory(segmentId);
        final SegmentBuilder<K, V> builder = Segment.<K, V>builder(
                segmentDirectory)//
                .withId(segmentId)//
                .withKeyTypeDescriptor(keyTypeDescriptor)//
                .withMaintenanceExecutor(maintenanceExecutor)//
                .withSegmentMaintenanceAutoEnabled(Boolean.TRUE
                        .equals(conf.isSegmentMaintenanceAutoEnabled()))//
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
                .withBloomFilterProbabilityOfFalsePositive(
                        conf.getBloomFilterProbabilityOfFalsePositive())//
                .withDiskIoBufferSize(conf.getDiskIoBufferSize())//
                .withEncodingChunkFilters(conf.getEncodingChunkFilters())//
                .withDecodingChunkFilters(conf.getDecodingChunkFilters());
        return builder;
    }

    SegmentFiles<K, V> newSegmentFiles(final SegmentId segmentId) {
        final AsyncDirectory segmentDirectory = openSegmentDirectory(segmentId);
        final long activeVersion = newSegmentPropertiesManager(segmentId)
                .getVersion();
        return new SegmentFiles<>(segmentDirectory, segmentId, keyTypeDescriptor,
                valueTypeDescriptor, conf.getDiskIoBufferSize(),
                conf.getEncodingChunkFilters(), conf.getDecodingChunkFilters(),
                activeVersion);
    }

    SegmentPropertiesManager newSegmentPropertiesManager(
            final SegmentId segmentId) {
        return new SegmentPropertiesManager(openSegmentDirectory(segmentId),
                segmentId);
    }

    private Segment<K, V> instantiateSegment(final SegmentId segmentId) {
        return newSegmentBuilder(segmentId).build();
    }

    protected void deleteSegmentFiles(final SegmentId segmentId) {
        deleteSegmentRootDirectory(segmentId);
    }

    void swapSegmentDirectories(final SegmentId segmentId,
            final SegmentId replacementSegmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Vldtn.requireNonNull(replacementSegmentId, "replacementSegmentId");
        directorySwap.swap(segmentId, replacementSegmentId);
        relabelSwappedSegment(segmentId, replacementSegmentId);
    }

    void recoverDirectorySwaps(final List<SegmentId> segmentIds) {
        Vldtn.requireNonNull(segmentIds, "segmentIds");
        for (final SegmentId segmentId : segmentIds) {
            directorySwap.recoverIfNeeded(segmentId);
        }
    }

    private void deleteSegmentRootDirectory(final SegmentId segmentId) {
        directorySwap.deleteSegmentRootDirectory(segmentId);
    }

    private AsyncDirectory openSegmentDirectory(final SegmentId segmentId) {
        return directoryFacade.openSubDirectory(segmentId.getName())
                .toCompletableFuture().join();
    }

    private void relabelSwappedSegment(final SegmentId targetSegmentId,
            final SegmentId sourceSegmentId) {
        final AsyncDirectory segmentDirectory = openSegmentDirectory(
                targetSegmentId);
        final SegmentPropertiesManager properties = new SegmentPropertiesManager(
                segmentDirectory, sourceSegmentId);
        final long activeVersion = properties.getVersion();
        final SegmentFiles<K, V> fromFiles = new SegmentFiles<>(
                segmentDirectory, sourceSegmentId, keyTypeDescriptor,
                valueTypeDescriptor, conf.getDiskIoBufferSize(),
                conf.getEncodingChunkFilters(), conf.getDecodingChunkFilters(),
                activeVersion);
        final SegmentFiles<K, V> toFiles = new SegmentFiles<>(
                segmentDirectory, targetSegmentId, keyTypeDescriptor,
                valueTypeDescriptor, conf.getDiskIoBufferSize(),
                conf.getEncodingChunkFilters(), conf.getDecodingChunkFilters(),
                activeVersion);
        filesRenamer.renameFiles(fromFiles, toFiles, properties);
    }

    private void closeSegmentIfNeeded(final Segment<K, V> segment) {
        if (segment != null && !segment.wasClosed()) {
            try {
                flushPendingWrites(segment);
            } catch (final RuntimeException e) {
                logger.warn("Failed to flush segment '{}' before close.",
                        segment.getId(), e);
            } finally {
                segment.close();
            }
        }
    }

    private void flushPendingWrites(final Segment<K, V> segment) {
        if (segment.getNumberOfKeysInWriteCache() == 0) {
            return;
        }
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentResult<CompletionStage<Void>> result = segment.flush();
            final SegmentResultStatus status = result.getStatus();
            if (status == SegmentResultStatus.OK) {
                awaitMaintenanceCompletion(segment.getId(), "flush",
                        result.getValue());
                return;
            }
            if (status == SegmentResultStatus.CLOSED) {
                return;
            }
            if (status == SegmentResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "flush",
                        segment.getId());
                continue;
            }
            throw new IndexException(String.format(
                    "Segment '%s' failed during flush: %s", segment.getId(),
                    status));
        }
    }

    private void awaitMaintenanceCompletion(final SegmentId segmentId,
            final String operation, final CompletionStage<Void> completion) {
        if (completion == null) {
            return;
        }
        try {
            completion.toCompletableFuture().join();
        } catch (final RuntimeException e) {
            throw new IndexException(String.format(
                    "Segment '%s' failed during %s.", segmentId, operation), e);
        }
    }

    private static int sanitizeRetryConf(final Integer configured,
            final int fallback) {
        if (configured == null || configured.intValue() < 1) {
            return fallback;
        }
        return configured.intValue();
    }

    private static boolean isSegmentLockConflict(
            final IllegalStateException exception) {
        final String message = exception.getMessage();
        return message != null && message.contains("already locked");
    }

    public void close() {
        final List<Segment<K, V>> toClose = new ArrayList<>();
        synchronized (segmentsLock) {
            if (!segments.isEmpty()) {
                toClose.addAll(segments.values());
                segments.clear();
            }
        }
        closeEvictedSegments(toClose);
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
        if (segments.size() <= maxNumberOfSegmentsInCache) {
            return;
        }
        final var iterator = segments.entrySet().iterator();
        while (segments.size() > maxNumberOfSegmentsInCache
                && iterator.hasNext()) {
            final Map.Entry<SegmentId, Segment<K, V>> eldest = iterator.next();
            if (splitsInFlight.contains(eldest.getKey())) {
                continue;
            }
            iterator.remove();
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
