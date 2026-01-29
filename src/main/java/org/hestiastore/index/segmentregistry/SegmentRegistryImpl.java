package org.hestiastore.index.segmentregistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
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
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentAsyncExecutor;
import org.hestiastore.index.segmentindex.SegmentSplitApplyPlan;
import org.hestiastore.index.segmentindex.SegmentSplitterResult;
import org.hestiastore.index.segmentindex.SplitAsyncExecutor;

/**
 * Registry that manages segment lifecycles and caches loaded segments.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class SegmentRegistryImpl<K, V> implements SegmentRegistry<K, V> {

    private final SegmentRegistryCache<K, V> cache = new SegmentRegistryCache<>();
    private final SegmentRegistryGate gate = new SegmentRegistryGate();
    private final Set<SegmentId> splitsInFlight = ConcurrentHashMap.newKeySet();
    private final Map<SegmentId, SegmentHandler<K, V>> handlers = new HashMap<>();

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

    public SegmentRegistryImpl(final AsyncDirectory directoryFacade,
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

    public ExecutorService getSplitExecutor() {
        return splitExecutor;
    }

    /**
     * Returns the segment for the provided id, loading it if needed.
     *
     * @param segmentId segment id to load
     * @return result containing the segment or a status
     */
    @Override
    public SegmentRegistryResult<Segment<K, V>> getSegment(
            final SegmentId segmentId) {
        final SegmentRegistryResult<SegmentHandler<K, V>> handlerResult = getSegmentHandler(
                segmentId);
        if (handlerResult.getStatus() == SegmentRegistryResultStatus.OK) {
            return handlerResult.getValue().getSegmentIfReady();
        }
        if (handlerResult.getStatus() == SegmentRegistryResultStatus.CLOSED) {
            return SegmentRegistryResult.closed();
        }
        if (handlerResult.getStatus() == SegmentRegistryResultStatus.ERROR) {
            return SegmentRegistryResult.error();
        }
        return SegmentRegistryResult.busy();
    }

    /**
     * Returns the segment handler for the provided id, loading the segment if needed.
     *
     * @param segmentId segment id to load
     * @return result containing the handler or a status
     */
    @Override
    public SegmentRegistryResult<SegmentHandler<K, V>> getSegmentHandler(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final SegmentRegistryState initialState = gate.getState();
        if (initialState != SegmentRegistryState.READY) {
            return resultForState(initialState);
        }
        final List<Segment<K, V>> evicted = new ArrayList<>();
        final SegmentRegistryResult<SegmentHandler<K, V>> result = cache
                .withLock(() -> {
                    final SegmentRegistryState state = gate.getState();
                    if (state != SegmentRegistryState.READY) {
                        return resultForState(state);
                    }
                    SegmentHandler<K, V> handler = handlers.get(segmentId);
                    if (handler != null
                            && handler.getState() == SegmentHandlerState.LOCKED) {
                        return SegmentRegistryResult.busy();
                    }
                    Segment<K, V> existing = cache.getLocked(segmentId);
                    final boolean needsCreate = existing == null
                            || existing.getState() == SegmentState.CLOSED;
                    final boolean needsEviction = !needsCreate
                            && cache.needsEvictionLocked(
                                    maxNumberOfSegmentsInCache,
                                    splitsInFlight);
                    if (needsCreate || needsEviction) {
                        if (state != SegmentRegistryState.READY) {
                            return resultForState(state);
                        }
                        try {
                            if (needsCreate) {
                                existing = instantiateSegment(segmentId);
                                cache.putLocked(segmentId, existing);
                            }
                            cache.evictIfNeededLocked(
                                    maxNumberOfSegmentsInCache, splitsInFlight,
                                    evicted);
                            removeHandlersForEvictedLocked(evicted);
                            handler = getOrCreateHandlerLocked(segmentId,
                                    existing);
                            if (handler
                                    .getState() == SegmentHandlerState.LOCKED) {
                                return SegmentRegistryResult.busy();
                            }
                            return SegmentRegistryResult.ok(handler);
                        } catch (final IllegalStateException e) {
                            if (isSegmentLockConflict(e)) {
                                return SegmentRegistryResult.busy();
                            }
                            throw e;
                        }
                    }
                    handler = getOrCreateHandlerLocked(segmentId, existing);
                    if (handler.getState() == SegmentHandlerState.LOCKED) {
                        return SegmentRegistryResult.busy();
                    }
                    return SegmentRegistryResult.ok(handler);
                });
        closeEvictedSegments(evicted);
        return result;
    }

    public void markSplitInFlight(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        splitsInFlight.add(segmentId);
    }

    public void clearSplitInFlight(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        splitsInFlight.remove(segmentId);
    }

    public boolean isSegmentInstance(final SegmentId segmentId,
            final Segment<K, V> expected) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Vldtn.requireNonNull(expected, "expected");
        return cache.withLock(
                () -> cache.isSegmentInstanceLocked(segmentId, expected));
    }

    public SegmentHandlerLockStatus lockSegmentHandler(final SegmentId segmentId,
            final Segment<K, V> expected) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Vldtn.requireNonNull(expected, "expected");
        return cache.withLock(() -> {
            final SegmentRegistryState state = gate.getState();
            if (state != SegmentRegistryState.READY) {
                return SegmentHandlerLockStatus.BUSY;
            }
            final Segment<K, V> current = cache.getLocked(segmentId);
            if (current != expected) {
                return SegmentHandlerLockStatus.BUSY;
            }
            final SegmentHandler<K, V> handler = getOrCreateHandlerLocked(
                    segmentId, current);
            return handler.lock();
        });
    }

    public void unlockSegmentHandler(final SegmentId segmentId,
            final Segment<K, V> expected) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Vldtn.requireNonNull(expected, "expected");
        cache.withLock(() -> {
            final SegmentHandler<K, V> handler = handlers.get(segmentId);
            if (handler == null || !handler.isForSegment(expected)) {
                throw new IllegalStateException(
                        "Segment handler mismatch.");
            }
            handler.unlock();
            final Segment<K, V> current = cache.getLocked(segmentId);
            if (current != expected) {
                handlers.remove(segmentId);
            }
        });
    }

    public void executeWithRegistryLock(final Runnable action) {
        Vldtn.requireNonNull(action, "action");
        cache.withLock(action);
    }

    public <T> T executeWithRegistryLock(final Supplier<T> action) {
        Vldtn.requireNonNull(action, "action");
        return cache.withLock(action);
    }

    public SegmentRegistryResult<Segment<K, V>> applySplitPlan(
            final SegmentSplitApplyPlan<K, V> plan,
            final Segment<K, V> lowerSegment,
            final Segment<K, V> upperSegment) {
        return applySplitPlan(plan, lowerSegment, upperSegment, null);
    }

    public SegmentRegistryResult<Segment<K, V>> applySplitPlan(
            final SegmentSplitApplyPlan<K, V> plan,
            final Segment<K, V> lowerSegment,
            final Segment<K, V> upperSegment,
            final BooleanSupplier onApplied) {
        Vldtn.requireNonNull(plan, "plan");
        if (Boolean.getBoolean("hestiastore.enforceSplitLockOrder")) {
            final String keyMapLock = System
                    .getProperty("hestiastore.keyMapLockHeld");
            final String registryLock = System
                    .getProperty("hestiastore.registryLockHeld");
            if ("true".equals(keyMapLock) && !"true".equals(registryLock)) {
                throw new IllegalStateException(
                        "Split apply requires registry lock before key-map lock.");
            }
        }
        validateSegmentIdMatch(lowerSegment, plan.getLowerSegmentId(),
                "lowerSegment");
        if (plan.getStatus() == SegmentSplitterResult.SegmentSplittingStatus.SPLIT) {
            final SegmentId upperId = Vldtn.requireNonNull(
                    plan.getUpperSegmentId().orElse(null), "upperSegmentId");
            validateSegmentIdMatch(upperSegment, upperId, "upperSegment");
        }
        return cache.withLock(() -> {
            final SegmentRegistryState state = gate.getState();
            if (state != SegmentRegistryState.READY) {
                return resultForState(state);
            }
            try (FreezeGuard guard = new FreezeGuard(gate)) {
                if (!guard.isActive()) {
                    return resultForState(gate.getState());
                }
                if (Boolean.getBoolean("hestiastore.enforceSplitLockOrder")) {
                    System.setProperty("hestiastore.registryLockHeld", "true");
                }
                try {
                    if (onApplied != null && !onApplied.getAsBoolean()) {
                        return SegmentRegistryResult.busy();
                    }
                    final Segment<K, V> removed = cache
                            .removeLocked(plan.getOldSegmentId());
                    final SegmentHandler<K, V> handler = handlers
                            .get(plan.getOldSegmentId());
                    if (handler == null
                            || handler.getState() != SegmentHandlerState.LOCKED) {
                        handlers.remove(plan.getOldSegmentId());
                    }
                    if (lowerSegment != null) {
                        cache.putLocked(plan.getLowerSegmentId(),
                                lowerSegment);
                        handlers.put(plan.getLowerSegmentId(),
                                new SegmentHandler<>(lowerSegment));
                    }
                    if (plan.getStatus() == SegmentSplitterResult.SegmentSplittingStatus.SPLIT
                            && upperSegment != null) {
                        final SegmentId upperId = plan.getUpperSegmentId()
                                .get();
                        cache.putLocked(upperId, upperSegment);
                        handlers.put(upperId,
                                new SegmentHandler<>(upperSegment));
                    }
                    return SegmentRegistryResult.ok(removed);
                } finally {
                    if (Boolean.getBoolean(
                            "hestiastore.enforceSplitLockOrder")) {
                        System.clearProperty("hestiastore.registryLockHeld");
                    }
                }
            }
        });
    }

    /**
     * Removes a segment from the registry and closes it.
     *
     * @param segmentId segment id to remove
     */
    @Override
    public void removeSegment(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final AtomicBoolean guardActive = new AtomicBoolean(false);
        final Segment<K, V> segment = cache.withLock(() -> {
            try (FreezeGuard guard = new FreezeGuard(gate)) {
                if (!guard.isActive()) {
                    return null;
                }
                guardActive.set(true);
                return removeSegmentFromRegistry(segmentId);
            }
        });
        if (!guardActive.get()) {
            return;
        }
        closeSegmentIfNeeded(segment);
        deleteSegmentFiles(segmentId);
    }

    /**
     * Evicts a segment only if the registry still points to the provided
     * instance.
     *
     * @param segmentId segment identifier to evict
     * @param expected  expected segment instance bound to the id
     * @return true when the segment was evicted, false otherwise
     */
    public boolean evictSegmentIfSame(final SegmentId segmentId,
            final Segment<K, V> expected) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Vldtn.requireNonNull(expected, "expected");
        final AtomicBoolean guardActive = new AtomicBoolean(false);
        final AtomicReference<Segment<K, V>> removed = new AtomicReference<>();
        cache.withLock(() -> {
            try (FreezeGuard guard = new FreezeGuard(gate)) {
                if (!guard.isActive()) {
                    return;
                }
                guardActive.set(true);
                final Segment<K, V> current = cache.getLocked(segmentId);
                if (current != expected) {
                    return;
                }
                removed.set(cache.removeLocked(segmentId));
                handlers.remove(segmentId);
            }
        });
        if (!guardActive.get()) {
            return false;
        }
        final Segment<K, V> removedSegment = removed.get();
        closeSegmentIfNeeded(removedSegment);
        return removedSegment != null;
    }

    public void closeSegmentInstance(final Segment<K, V> segment) {
        closeSegmentIfNeeded(segment);
    }

    private SegmentHandler<K, V> getOrCreateHandlerLocked(
            final SegmentId segmentId, final Segment<K, V> segment) {
        final SegmentHandler<K, V> existing = handlers.get(segmentId);
        if (existing != null && existing.isForSegment(segment)) {
            return existing;
        }
        final SegmentHandler<K, V> handler = new SegmentHandler<>(segment);
        handlers.put(segmentId, handler);
        return handler;
    }

    private void removeHandlersForEvictedLocked(
            final List<Segment<K, V>> evicted) {
        if (evicted.isEmpty()) {
            return;
        }
        handlers.entrySet().removeIf(entry -> {
            if (entry.getValue().getState() == SegmentHandlerState.LOCKED) {
                return false;
            }
            for (final Segment<K, V> segment : evicted) {
                if (entry.getValue().isForSegment(segment)) {
                    return true;
                }
            }
            return false;
        });
    }


    private Segment<K, V> removeSegmentFromRegistry(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        handlers.remove(segmentId);
        return cache.removeLocked(segmentId);
    }

    public SegmentBuilder<K, V> newSegmentBuilder(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final AsyncDirectory segmentDirectory = openSegmentDirectory(segmentId);
        final SegmentBuilder<K, V> builder = Segment
                .<K, V>builder(segmentDirectory)//
                .withId(segmentId)//
                .withKeyTypeDescriptor(keyTypeDescriptor)//
                .withMaintenanceExecutor(maintenanceExecutor)//
                .withSegmentMaintenanceAutoEnabled(Boolean.TRUE
                        .equals(conf.isSegmentMaintenanceAutoEnabled()))//
                .withMaxNumberOfKeysInSegmentWriteCache(
                        conf.getMaxNumberOfKeysInSegmentWriteCache().intValue())//
                .withMaxNumberOfKeysInSegmentCache(
                        conf.getMaxNumberOfKeysInSegmentCache())//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(conf
                        .getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance()
                        .intValue())//
                .withMaxNumberOfKeysInSegmentChunk(
                        conf.getMaxNumberOfKeysInSegmentChunk())//
                .withMaxNumberOfDeltaCacheFiles(
                        conf.getMaxNumberOfDeltaCacheFiles())//
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

    private Segment<K, V> instantiateSegment(final SegmentId segmentId) {
        return newSegmentBuilder(segmentId).build();
    }

    public void deleteSegmentFiles(final SegmentId segmentId) {
        deleteSegmentRootDirectory(segmentId);
    }

    public void swapSegmentDirectories(final SegmentId segmentId,
            final SegmentId replacementSegmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Vldtn.requireNonNull(replacementSegmentId, "replacementSegmentId");
        directorySwap.swap(segmentId, replacementSegmentId);
        relabelSwappedSegment(segmentId, replacementSegmentId);
    }

    public void recoverDirectorySwaps(final List<SegmentId> segmentIds) {
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
        final SegmentFiles<K, V> toFiles = new SegmentFiles<>(segmentDirectory,
                targetSegmentId, keyTypeDescriptor, valueTypeDescriptor,
                conf.getDiskIoBufferSize(), conf.getEncodingChunkFilters(),
                conf.getDecodingChunkFilters(), activeVersion);
        filesRenamer.renameFiles(fromFiles, toFiles, properties);
    }

    private void closeSegmentIfNeeded(final Segment<K, V> segment) {
        if (segment == null) {
            return;
        }
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentState state = segment.getState();
            if (state == SegmentState.CLOSED) {
                return;
            }
            if (state == SegmentState.ERROR) {
                throw new IndexException(
                        String.format("Segment '%s' failed during close: %s",
                                segment.getId(), state));
            }
            final SegmentResult<Void> result = segment.close();
            final SegmentResultStatus status = result.getStatus();
            if (status == SegmentResultStatus.OK) {
                awaitSegmentClosed(segment, startNanos);
                return;
            }
            if (status == SegmentResultStatus.CLOSED) {
                return;
            }
            if (status == SegmentResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "close",
                        segment.getId());
                continue;
            }
            throw new IndexException(
                    String.format("Segment '%s' failed during close: %s",
                            segment.getId(), status));
        }
    }

    private void awaitSegmentClosed(final Segment<K, V> segment,
            final long startNanos) {
        while (true) {
            final SegmentState state = segment.getState();
            if (state == SegmentState.CLOSED) {
                return;
            }
            if (state == SegmentState.ERROR) {
                throw new IndexException(
                        String.format("Segment '%s' failed during close: %s",
                                segment.getId(), state));
            }
            retryPolicy.backoffOrThrow(startNanos, "close", segment.getId());
        }
    }

    private static int sanitizeRetryConf(final Integer configured,
            final int fallback) {
        if (configured == null || configured.intValue() < 1) {
            return fallback;
        }
        return configured.intValue();
    }

    private static <T> SegmentRegistryResult<T> resultForState(
            final SegmentRegistryState state) {
        if (state == SegmentRegistryState.CLOSED) {
            return SegmentRegistryResult.closed();
        }
        if (state == SegmentRegistryState.ERROR) {
            return SegmentRegistryResult.error();
        }
        return SegmentRegistryResult.busy();
    }

    private static boolean isSegmentLockConflict(
            final IllegalStateException exception) {
        final String message = exception.getMessage();
        return message != null && message.contains("already locked");
    }

    private static <K, V> void validateSegmentIdMatch(
            final Segment<K, V> segment, final SegmentId expectedId,
            final String propertyName) {
        if (segment == null) {
            return;
        }
        final SegmentId actual = Vldtn.requireNonNull(segment.getId(),
                propertyName);
        if (!expectedId.equals(actual)) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' must match id '%s'. Got: '%s'",
                    propertyName, expectedId, actual));
        }
    }

    private static final class FreezeGuard implements AutoCloseable {
        private final SegmentRegistryGate gate;
        private final boolean active;

        private FreezeGuard(final SegmentRegistryGate gate) {
            this.gate = gate;
            this.active = gate.tryEnterFreeze();
        }

        private boolean isActive() {
            return active;
        }

        @Override
        public void close() {
            if (active) {
                gate.finishFreezeToReady();
            }
        }
    }

    /**
     * Closes all tracked segments and releases executors.
     */
    @Override
    public void close() {
        gate.close();
        final List<Segment<K, V>> toClose = cache.withLock(() -> {
            final List<Segment<K, V>> snapshot = cache
                    .snapshotAndClearLocked();
            handlers.clear();
            return snapshot;
        });
        closeEvictedSegments(toClose);
        if (!segmentAsyncExecutor.wasClosed()) {
            segmentAsyncExecutor.close();
        }
        if (!splitAsyncExecutor.wasClosed()) {
            splitAsyncExecutor.close();
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
