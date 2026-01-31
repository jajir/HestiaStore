package org.hestiastore.index.segmentregistry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
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

    private static final Pattern SEGMENT_DIR_PATTERN = Pattern
            .compile("^segment-\\d{5}$");

    private final SegmentRegistryCache<K, V> cache = new SegmentRegistryCache<>();
    private final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();

    private final AsyncDirectory directoryFacade;
    private final SegmentFactory<K, V> segmentFactory;
    private final SegmentIdAllocator segmentIdAllocator;
    private final int maxNumberOfSegmentsInCache;
    private final IndexRetryPolicy retryPolicy;

    /**
     * Creates a registry backed by the provided directory and configuration.
     *
     * @param directoryFacade    async directory facade
     * @param segmentFactory     factory for creating segment instances
     * @param segmentIdAllocator allocator for new segment ids
     * @param conf               index configuration
     */
    SegmentRegistryImpl(final AsyncDirectory directoryFacade,
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
    public SegmentRegistryAccess<SegmentId> allocateSegmentId() {
        final SegmentRegistryState state = gate.getState();
        if (state != SegmentRegistryState.READY) {
            return SegmentRegistryAccessImpl
                    .forStatus(resultForState(state).getStatus());
        }
        final SegmentId segmentId;
        try {
            segmentId = segmentIdAllocator.nextId();
        } catch (final RuntimeException e) {
            return SegmentRegistryAccessImpl
                    .forStatus(SegmentRegistryResultStatus.ERROR);
        }
        if (segmentId == null) {
            return SegmentRegistryAccessImpl
                    .forStatus(SegmentRegistryResultStatus.ERROR);
        }
        return SegmentRegistryAccessImpl
                .forValue(SegmentRegistryResultStatus.OK, segmentId);
    }

    /**
     * Returns the segment for the provided id, loading it if needed.
     *
     * @param segmentId segment id to load
     * @return result containing the segment or a status
     */
    @Override
    public SegmentRegistryAccess<Segment<K, V>> getSegment(
            final SegmentId segmentId) {
        return loadSegment(segmentId, false);
    }

    /**
     * Creates and registers a new segment using a freshly allocated id.
     *
     * @return registry result containing the new segment or a status
     */
    @Override
    public SegmentRegistryAccess<Segment<K, V>> createSegment() {
        final SegmentRegistryAccess<SegmentId> idResult = allocateSegmentId();
        if (idResult.getSegmentStatus() == SegmentRegistryResultStatus.OK) {
            final SegmentId segmentId = idResult.getSegment().orElse(null);
            if (segmentId == null) {
                return SegmentRegistryAccessImpl
                        .forStatus(SegmentRegistryResultStatus.ERROR);
            }
            return loadSegment(segmentId, true);
        }
        return SegmentRegistryAccessImpl.forStatus(idResult.getSegmentStatus());
    }

    /**
     * Returns the segment for the provided id, with optional create-on-miss.
     *
     * @param segmentId              segment id to load
     * @param allowCreateWhenMissing whether to create missing segments
     * @return result containing the segment or a status
     */
    private SegmentRegistryAccess<Segment<K, V>> loadSegment(
            final SegmentId segmentId, final boolean allowCreateWhenMissing) {
        final SegmentRegistryResult<SegmentHandler<K, V>> handlerResult = getSegmentHandler(
                segmentId, allowCreateWhenMissing);
        if (handlerResult.getStatus() == SegmentRegistryResultStatus.OK) {
            return SegmentRegistryAccessImpl.forHandler(
                    SegmentRegistryResultStatus.OK, handlerResult.getValue());
        }
        return SegmentRegistryAccessImpl.forStatus(handlerResult.getStatus());
    }

    /**
     * Returns the segment handler for the provided id, loading the segment if
     * needed.
     *
     * @param segmentId              segment id to load
     * @param allowCreateWhenMissing whether to create missing segments
     * @return result containing the handler or a status
     */
    private SegmentRegistryResult<SegmentHandler<K, V>> getSegmentHandler(
            final SegmentId segmentId, final boolean allowCreateWhenMissing) {
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
                    SegmentHandler<K, V> handler = cache.getLocked(segmentId);
                    if (handler != null && handler
                            .getState() == SegmentHandlerState.LOCKED) {
                        return SegmentRegistryResult.busy();
                    }
                    Segment<K, V> existing = handler == null ? null
                            : handler.getSegment();
                    final boolean needsCreate = existing == null
                            || existing.getState() == SegmentState.CLOSED;
                    if (needsCreate) {
                        if (state != SegmentRegistryState.READY) {
                            return resultForState(state);
                        }
                        try {
                            if (!allowCreateWhenMissing
                                    && !segmentDirectoryExists(segmentId)
                                    && hasAnySegmentDirectories()) {
                                return SegmentRegistryResult.notFound();
                            }
                            existing = instantiateSegment(segmentId);
                            handler = new SegmentHandler<>(existing);
                            cache.putLocked(segmentId, handler);
                        } catch (final IllegalStateException e) {
                            if (isSegmentLockConflict(e)) {
                                return SegmentRegistryResult.busy();
                            }
                            throw e;
                        }
                    }
                    if (cache.needsEvictionLocked(maxNumberOfSegmentsInCache)) {
                        cache.evictIfNeededLocked(maxNumberOfSegmentsInCache,
                                evicted);
                    }
                    if (handler == null) {
                        handler = cache.getLocked(segmentId);
                    }
                    if (handler == null) {
                        return SegmentRegistryResult.error();
                    }
                    if (handler.getState() == SegmentHandlerState.LOCKED) {
                        return SegmentRegistryResult.busy();
                    }
                    return SegmentRegistryResult.ok(handler);
                });
        closeEvictedSegments(evicted);
        return result;
    }

    /**
     * Removes a segment from the registry and closes it.
     *
     * @param segmentId segment id to remove
     */
    @Override
    public SegmentRegistryAccess<Void> deleteSegment(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final AtomicReference<SegmentRegistryResultStatus> statusRef = new AtomicReference<>();
        final AtomicReference<SegmentHandler<K, V>> handlerRef = new AtomicReference<>();
        cache.withLock(() -> {
            final SegmentRegistryState state = gate.getState();
            if (state != SegmentRegistryState.READY) {
                statusRef.set(resultForState(state).getStatus());
                return;
            }
            final SegmentHandler<K, V> handler = cache.getLocked(segmentId);
            if (handler == null) {
                return;
            }
            if (handler.getState() == SegmentHandlerState.LOCKED) {
                statusRef.set(SegmentRegistryResultStatus.BUSY);
                return;
            }
            if (handler.lock() != SegmentHandlerLockStatus.OK) {
                statusRef.set(SegmentRegistryResultStatus.BUSY);
                return;
            }
            handlerRef.set(handler);
        });
        if (statusRef.get() != null) {
            return SegmentRegistryAccessImpl.forStatus(statusRef.get());
        }
        final SegmentHandler<K, V> handler = handlerRef.get();
        final Segment<K, V> segment = handler == null ? null
                : handler.getSegment();
        try {
            closeSegmentIfNeeded(segment);
            deleteSegmentFilesInternal(segmentId);
        } finally {
            if (handler != null) {
                final AtomicBoolean removed = new AtomicBoolean();
                cache.withLock(() -> {
                    final SegmentHandler<K, V> current = cache
                            .getLocked(segmentId);
                    if (current == handler) {
                        cache.removeLocked(segmentId);
                        removed.set(true);
                    }
                });
                if (!removed.get()) {
                    handler.unlock();
                }
            }
        }
        return SegmentRegistryAccessImpl
                .forStatus(SegmentRegistryResultStatus.OK);
    }

    private boolean segmentDirectoryExists(final SegmentId segmentId) {
        return exists(segmentId.getName());
    }

    private boolean hasAnySegmentDirectories() {
        try (Stream<String> names = directoryFacade.getFileNamesAsync()
                .toCompletableFuture().join()) {
            return names.anyMatch(SegmentRegistryImpl::isSegmentDirectoryName);
        }
    }

    private static boolean isSegmentDirectoryName(final String name) {
        if (name == null || name.isBlank()) {
            return false;
        }
        return SEGMENT_DIR_PATTERN.matcher(name).matches();
    }

    private Segment<K, V> instantiateSegment(final SegmentId segmentId) {
        return segmentFactory.buildSegment(segmentId);
    }

    /**
     * Deletes segment files for the provided id.
     *
     * @param segmentId segment id to delete
     */
    private void deleteSegmentFilesInternal(final SegmentId segmentId) {
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

    /**
     * Closes all tracked segments.
     */
    @Override
    public SegmentRegistryAccess<Void> close() {
        gate.close();
        final List<Segment<K, V>> toClose = cache.withLock(() -> {
            final List<Segment<K, V>> snapshot = cache.snapshotAndClearLocked();
            return snapshot;
        });
        closeEvictedSegments(toClose);
        return SegmentRegistryAccessImpl
                .forStatus(SegmentRegistryResultStatus.OK);
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
