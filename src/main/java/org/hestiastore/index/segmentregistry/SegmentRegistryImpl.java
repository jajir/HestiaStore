package org.hestiastore.index.segmentregistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;

/**
 * Registry that manages segment lifecycles and caches loaded segments.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class SegmentRegistryImpl<K, V> implements SegmentRegistry<K, V> {

    private final SegmentRegistryCache<K, V> cache = new SegmentRegistryCache<>();
    private final SegmentRegistryGate gate = new SegmentRegistryGate();
    private final Map<SegmentId, SegmentHandler<K, V>> handlers = new HashMap<>();

    private final AsyncDirectory directoryFacade;
    private final SegmentFactory<K, V> segmentFactory;
    private final SegmentIdAllocator segmentIdAllocator;
    private final int maxNumberOfSegmentsInCache;
    private final IndexRetryPolicy retryPolicy;

    /**
     * Creates a registry backed by the provided directory and configuration.
     *
     * @param directoryFacade   async directory facade
     * @param segmentFactory    factory for creating segment instances
     * @param segmentIdAllocator allocator for new segment ids
     * @param conf              index configuration
     */
    public SegmentRegistryImpl(final AsyncDirectory directoryFacade,
            final SegmentFactory<K, V> segmentFactory,
            final SegmentIdAllocator segmentIdAllocator,
            final IndexConfiguration<K, V> conf) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.segmentFactory = Vldtn.requireNonNull(segmentFactory,
                "segmentFactory");
        this.segmentIdAllocator = Vldtn.requireNonNull(segmentIdAllocator,
                "segmentIdAllocator");
        Vldtn.requireNonNull(conf, "conf");
        final int maxSegments = Vldtn
                .requireNonNull(conf.getMaxNumberOfSegmentsInCache(),
                        "maxNumberOfSegmentsInCache")
                .intValue();
        this.maxNumberOfSegmentsInCache = Vldtn.requireGreaterThanZero(
                maxSegments, "maxNumberOfSegmentsInCache");
        final int busyBackoffMillis = sanitizeRetryConf(
                conf.getIndexBusyBackoffMillis(),
                IndexConfigurationContract.DEFAULT_INDEX_BUSY_BACKOFF_MILLIS);
        final int busyTimeoutMillis = sanitizeRetryConf(
                conf.getIndexBusyTimeoutMillis(),
                IndexConfigurationContract.DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS);
        this.retryPolicy = new IndexRetryPolicy(busyBackoffMillis,
                busyTimeoutMillis);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SegmentRegistryResult<SegmentId> allocateSegmentId() {
        final SegmentRegistryState state = gate.getState();
        if (state != SegmentRegistryState.READY) {
            return resultForState(state);
        }
        final SegmentId segmentId;
        try {
            segmentId = segmentIdAllocator.nextId();
        } catch (final RuntimeException e) {
            return SegmentRegistryResult.error();
        }
        if (segmentId == null) {
            return SegmentRegistryResult.error();
        }
        return SegmentRegistryResult.ok(segmentId);
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
     * Returns the segment handler for the provided id, loading the segment if
     * needed.
     *
     * @param segmentId segment id to load
     * @return result containing the handler or a status
     */
    private SegmentRegistryResult<SegmentHandler<K, V>> getSegmentHandler(
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
                    if (handler != null && handler
                            .getState() == SegmentHandlerState.LOCKED) {
                        return SegmentRegistryResult.busy();
                    }
                    Segment<K, V> existing = cache.getLocked(segmentId);
                    final boolean needsCreate = existing == null
                            || existing.getState() == SegmentState.CLOSED;
                    final Set<SegmentId> protectedIds = lockedHandlerIdsLocked();
                    final boolean needsEviction = !needsCreate
                            && cache.needsEvictionLocked(
                                    maxNumberOfSegmentsInCache, protectedIds);
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
                                    maxNumberOfSegmentsInCache, protectedIds,
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

    /**
     * Returns true if the registry still maps the id to the provided segment.
     *
     * @param segmentId segment id to verify
     * @param expected  expected segment instance
     * @return true when the instance matches
     */
    public boolean isSegmentInstance(final SegmentId segmentId,
            final Segment<K, V> expected) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Vldtn.requireNonNull(expected, "expected");
        return cache.withLock(
                () -> cache.isSegmentInstanceLocked(segmentId, expected));
    }

    /**
     * Attempts to lock the handler for the provided segment id.
     *
     * @param segmentId segment id to lock
     * @param expected  expected segment instance
     * @return lock status indicating success or BUSY
     */
    public SegmentHandlerLockStatus lockSegmentHandler(
            final SegmentId segmentId, final Segment<K, V> expected) {
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

    /**
     * Unlocks the handler for the provided segment id.
     *
     * @param segmentId segment id to unlock
     * @param expected  expected segment instance
     */
    public void unlockSegmentHandler(final SegmentId segmentId,
            final Segment<K, V> expected) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Vldtn.requireNonNull(expected, "expected");
        cache.withLock(() -> {
            final SegmentHandler<K, V> handler = handlers.get(segmentId);
            if (handler == null || !handler.isForSegment(expected)) {
                throw new IllegalStateException("Segment handler mismatch.");
            }
            handler.unlock();
            final Segment<K, V> current = cache.getLocked(segmentId);
            if (current != expected) {
                handlers.remove(segmentId);
            }
        });
    }

    /**
     * Removes a segment from the registry and closes it.
     *
     * @param segmentId segment id to remove
     */
    @Override
    public SegmentRegistryResult<Void> deleteSegment(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final AtomicReference<SegmentRegistryResult<Void>> statusRef = new AtomicReference<>();
        final Segment<K, V> segment = cache.withLock(() -> {
            final SegmentRegistryState state = gate.getState();
            if (state != SegmentRegistryState.READY) {
                statusRef.set(resultForState(state));
                return null;
            }
            final SegmentHandler<K, V> handler = handlers.get(segmentId);
            if (handler != null
                    && handler.getState() == SegmentHandlerState.LOCKED) {
                statusRef.set(SegmentRegistryResult.busy());
                return null;
            }
            try (FreezeGuard guard = new FreezeGuard(gate)) {
                if (!guard.isActive()) {
                    statusRef.set(resultForState(gate.getState()));
                    return null;
                }
                return removeSegmentFromRegistry(segmentId);
            }
        });
        if (statusRef.get() != null) {
            return statusRef.get();
        }
        closeSegmentIfNeeded(segment);
        deleteSegmentFiles(segmentId);
        return SegmentRegistryResult.ok();
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

    private Set<SegmentId> lockedHandlerIdsLocked() {
        if (handlers.isEmpty()) {
            return Set.of();
        }
        final Set<SegmentId> protectedIds = new HashSet<>();
        for (final Map.Entry<SegmentId, SegmentHandler<K, V>> entry : handlers
                .entrySet()) {
            if (entry.getValue().getState() == SegmentHandlerState.LOCKED) {
                protectedIds.add(entry.getKey());
            }
        }
        return protectedIds;
    }

    private Segment<K, V> removeSegmentFromRegistry(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        handlers.remove(segmentId);
        return cache.removeLocked(segmentId);
    }

    private Segment<K, V> instantiateSegment(final SegmentId segmentId) {
        return segmentFactory.buildSegment(segmentId);
    }

    /**
     * Deletes segment files for the provided id.
     *
     * @param segmentId segment id to delete
     */
    private void deleteSegmentFiles(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        deleteSegmentRootDirectory(segmentId);
    }

    private void deleteSegmentRootDirectory(final SegmentId segmentId) {
        deleteDirectory(segmentId.getName());
    }

    private void deleteDirectory(final String directoryName) {
        if (!exists(directoryName)) {
            return;
        }
        final AsyncDirectory directory = directoryFacade
                .openSubDirectory(directoryName).toCompletableFuture().join();
        clearDirectory(directory);
        try {
            directoryFacade.rmdir(directoryName).toCompletableFuture().join();
        } catch (final RuntimeException e) {
            // Best-effort cleanup.
        }
    }

    private void clearDirectory(final AsyncDirectory directory) {
        try (Stream<String> files = directory.getFileNamesAsync()
                .toCompletableFuture().join()) {
            files.forEach(fileName -> {
                boolean deleted = false;
                try {
                    deleted = directory.deleteFileAsync(fileName)
                            .toCompletableFuture().join();
                    if (deleted) {
                        return;
                    }
                } catch (final RuntimeException e) {
                    // fall through to directory cleanup
                }
                try {
                    if (!directory.isFileExistsAsync(fileName)
                            .toCompletableFuture().join()) {
                        return;
                    }
                } catch (final RuntimeException e) {
                    return;
                }
                try {
                    final AsyncDirectory subDirectory = directory
                            .openSubDirectory(fileName).toCompletableFuture()
                            .join();
                    clearDirectory(subDirectory);
                    directory.rmdir(fileName).toCompletableFuture().join();
                } catch (final RuntimeException e) {
                    // Best-effort cleanup.
                }
            });
        }
    }

    private boolean exists(final String fileName) {
        return directoryFacade.isFileExistsAsync(fileName).toCompletableFuture()
                .join();
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
     * Closes all tracked segments.
     */
    @Override
    public SegmentRegistryResult<Void> close() {
        gate.close();
        final List<Segment<K, V>> toClose = cache.withLock(() -> {
            final List<Segment<K, V>> snapshot = cache.snapshotAndClearLocked();
            handlers.clear();
            return snapshot;
        });
        closeEvictedSegments(toClose);
        return SegmentRegistryResult.ok();
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
