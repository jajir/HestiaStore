package org.hestiastore.index.segmentregistry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * <p>
 * Design contract follows {@code docs/architecture/registry.md}:
 * state-gated request handling, per-key cache coordination, and
 * exception-driven load/open failures.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class SegmentRegistryImpl<K, V> implements SegmentRegistry<K, V> {

    private final SegmentRegistryCache<SegmentId, SegmentHandler<K, V>> cache;
    private final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();
    private final AtomicBoolean closeInProgress = new AtomicBoolean();
    private final ThreadLocal<Boolean> allowCreateOnMiss = ThreadLocal
            .withInitial(() -> Boolean.FALSE);

    private final SegmentFactory<K, V> segmentFactory;
    private final SegmentIdAllocator segmentIdAllocator;
    private final IndexRetryPolicy retryPolicy;
    private final ExecutorService segmentLifecycleExecutor;
    private final SegmentRegistryFileSystem fileSystem;

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
            final IndexConfiguration<K, V> conf,
            final ExecutorService segmentLifecycleExecutor) {
        final AsyncDirectory resolvedDirectory = Vldtn
                .requireNonNull(directoryFacade, "directoryFacade");
        this.segmentFactory = Vldtn.requireNonNull(segmentFactory,
                "segmentFactory");
        this.segmentIdAllocator = Vldtn.requireNonNull(segmentIdAllocator,
                "segmentIdAllocator");
        this.segmentLifecycleExecutor = Vldtn.requireNonNull(
                segmentLifecycleExecutor, "segmentLifecycleExecutor");
        this.fileSystem = new SegmentRegistryFileSystem(resolvedDirectory);
        Vldtn.requireNonNull(conf, "conf");
        final int maxSegments = Vldtn
                .requireNonNull(conf.getMaxNumberOfSegmentsInCache(),
                        "maxNumberOfSegmentsInCache")
                .intValue();
        final int maxNumberOfSegmentsInCache = Vldtn.requireGreaterThanZero(
                maxSegments, "maxNumberOfSegmentsInCache");
        final int busyBackoffMillis = sanitizeRetryConf(
                conf.getIndexBusyBackoffMillis(),
                IndexConfigurationContract.DEFAULT_INDEX_BUSY_BACKOFF_MILLIS);
        final int busyTimeoutMillis = sanitizeRetryConf(
                conf.getIndexBusyTimeoutMillis(),
                IndexConfigurationContract.DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS);
        this.retryPolicy = new IndexRetryPolicy(busyBackoffMillis,
                busyTimeoutMillis);
        this.cache = new SegmentRegistryCache<>(maxNumberOfSegmentsInCache,
                this::loadSegmentHandlerDirect,
                this::closeHandlerDirect, segmentLifecycleExecutor);
        if (!gate.finishFreezeToReady()) {
            throw new IllegalStateException(
                    "Failed to transition registry from FREEZE to READY");
        }
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
     * <p>
     * When gate is not READY this method maps state to BUSY/CLOSED/ERROR
     * without entering the cache path.
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
                    SegmentRegistryResultStatus.OK, handlerResult.getValue(),
                    cache, segmentId);
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
        final SegmentRegistryState state = gate.getState();
        if (state != SegmentRegistryState.READY) {
            return resultForState(state);
        }
        final boolean previousAllowCreate = allowCreateOnMiss.get();
        allowCreateOnMiss.set(allowCreateWhenMissing);
        try {
            while (true) {
                final SegmentHandler<K, V> handler;
                try {
                    handler = cache.get(segmentId);
                } catch (final SegmentNotFoundException ex) {
                    return SegmentRegistryResult.busy();
                } catch (final SegmentRegistryCache.EntryBusyException ex) {
                    return SegmentRegistryResult.busy();
                } catch (final SegmentBusyException ex) {
                    return SegmentRegistryResult.busy();
                }
                if (handler == null) {
                    return SegmentRegistryResult.error();
                }
                if (handler.getState() == SegmentHandlerState.LOCKED) {
                    return SegmentRegistryResult.busy();
                }
                final Segment<K, V> segment = handler.getSegment();
                if (segment == null) {
                    cache.invalidate(segmentId);
                    continue;
                }
                if (segment.getState() == SegmentState.CLOSED) {
                    cache.invalidate(segmentId);
                    continue;
                }
                return SegmentRegistryResult.ok(handler);
            }
        } finally {
            allowCreateOnMiss.set(previousAllowCreate);
        }
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
        final SegmentRegistryState state = gate.getState();
        if (state != SegmentRegistryState.READY) {
            return SegmentRegistryAccessImpl
                    .forStatus(resultForState(state).getStatus());
        }
        final SegmentRegistryCache.InvalidateStatus status = cache
                .invalidate(segmentId);
        if (status == SegmentRegistryCache.InvalidateStatus.BUSY) {
            return SegmentRegistryAccessImpl
                    .forStatus(SegmentRegistryResultStatus.BUSY);
        }
        fileSystem.deleteSegmentFiles(segmentId);
        return SegmentRegistryAccessImpl
                .forStatus(SegmentRegistryResultStatus.OK);
    }

    private Segment<K, V> instantiateSegment(final SegmentId segmentId) {
        return segmentFactory.buildSegment(segmentId);
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
        // Contract mapping:
        // CLOSED -> CLOSED, ERROR -> ERROR, otherwise (FREEZE) -> BUSY.
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
     * <p>
     * Close is idempotent. Runtime close failures while unloading are handled by
     * cache/entry semantics and may leave entries unavailable (BUSY) by design.
     */
    @Override
    public SegmentRegistryAccess<Void> close() {
        try {
            final SegmentRegistryState initialState = gate.getState();
            if (initialState == SegmentRegistryState.ERROR) {
                return SegmentRegistryAccessImpl
                        .forStatus(SegmentRegistryResultStatus.ERROR);
            }
            if (initialState == SegmentRegistryState.CLOSED) {
                return SegmentRegistryAccessImpl
                        .forStatus(SegmentRegistryResultStatus.OK);
            }
            if (!closeInProgress.compareAndSet(false, true)) {
                final SegmentRegistryState state = gate.getState();
                if (state == SegmentRegistryState.CLOSED) {
                    return SegmentRegistryAccessImpl
                            .forStatus(SegmentRegistryResultStatus.OK);
                }
                if (state == SegmentRegistryState.ERROR) {
                    return SegmentRegistryAccessImpl
                            .forStatus(SegmentRegistryResultStatus.ERROR);
                }
                return SegmentRegistryAccessImpl
                        .forStatus(SegmentRegistryResultStatus.BUSY);
            }
            if (gate.getState() == SegmentRegistryState.READY) {
                gate.tryEnterFreeze();
            }
            final SegmentRegistryState freezeState = gate.getState();
            if (freezeState == SegmentRegistryState.ERROR) {
                return SegmentRegistryAccessImpl
                        .forStatus(SegmentRegistryResultStatus.ERROR);
            }
            if (freezeState == SegmentRegistryState.CLOSED) {
                return SegmentRegistryAccessImpl
                        .forStatus(SegmentRegistryResultStatus.OK);
            }
            if (freezeState != SegmentRegistryState.FREEZE) {
                return SegmentRegistryAccessImpl
                        .forStatus(SegmentRegistryResultStatus.BUSY);
            }
            cache.clear();
            if (!gate.finishFreezeToClosed()) {
                return SegmentRegistryAccessImpl
                        .forStatus(SegmentRegistryResultStatus.ERROR);
            }
            return SegmentRegistryAccessImpl
                    .forStatus(SegmentRegistryResultStatus.OK);
        } finally {
            segmentLifecycleExecutor.shutdownNow();
        }
    }

    private SegmentHandler<K, V> loadSegmentHandlerDirect(
            final SegmentId segmentId) {
        final SegmentRegistryState state = gate.getState();
        if (state != SegmentRegistryState.READY) {
            throw new SegmentBusyException("Registry state is " + state);
        }
        final boolean allowCreate = Boolean.TRUE
                .equals(allowCreateOnMiss.get());
        if (!allowCreate && !fileSystem.segmentDirectoryExists(segmentId)
                && fileSystem.hasAnySegmentDirectories()) {
            throw new SegmentNotFoundException();
        }
        try {
            return new SegmentHandler<>(instantiateSegment(segmentId));
        } catch (final IllegalStateException e) {
            if (isSegmentLockConflict(e)) {
                throw new SegmentBusyException(e.getMessage(), e);
            }
            throw e;
        }
    }

    private void closeHandlerDirect(final SegmentHandler<K, V> handler) {
        if (handler == null) {
            return;
        }
        closeSegmentIfNeeded(handler.getSegment());
    }

    private static final class SegmentNotFoundException
            extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private static final class SegmentBusyException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private SegmentBusyException(final String message) {
            super(message);
        }

        private SegmentBusyException(final String message,
                final Throwable cause) {
            super(message, cause);
        }
    }

}
