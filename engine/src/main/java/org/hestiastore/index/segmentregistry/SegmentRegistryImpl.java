package org.hestiastore.index.segmentregistry;

import java.util.Optional;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry that manages segment lifecycles and caches loaded segments.
 * <p>
 * Design contract follows {@code docs/architecture/registry.md}:
 * state-gated request handling, per-key cache coordination, and
 * status-driven load/open outcomes.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentRegistryImpl<K, V>
        implements SegmentRegistry<K, V>, SegmentRegistryStatusAccess<K, V> {

    private static final Logger logger = LoggerFactory
            .getLogger(SegmentRegistryImpl.class);
    private static final String SEGMENT_ID_PARAMETER = "segmentId";

    private final SegmentRegistryCache<SegmentId, Segment<K, V>> cache;
    private final SegmentRegistryStateMachine gate;
    private final BusyRetryPolicy closeRetryPolicy;
    private final BusyRetryPolicy blockingRetryPolicy;

    private final SegmentIdAllocator segmentIdAllocator;
    private final SegmentRegistryFileSystem fileSystem;
    private final PreparedSegmentWriterFactory<K, V> preparedSegmentWriterFactory;
    private final SegmentRuntimeTuner runtimeTuner;
    private final BlockingSegmentRegistryAdapter<K, V> blockingFacade;
    private final SegmentRegistry.Materialization<K, V> materialization;
    private final SegmentRegistry.Runtime<K, V> runtime;
    private final Set<SegmentId> pinnedSegments;
    private final ConcurrentMap<SegmentId, SegmentHandle<K, V>> handleCache;

    /**
     * Creates a registry using prebuilt dependencies from the builder.
     *
     * @param segmentIdAllocator allocator for new segment ids
     * @param fileSystem file system facade for segment directories/files
     * @param cache prebuilt segment cache
     * @param closeRetryPolicy retry policy for draining cache on close
     * @param gate registry gate state machine shared by collaborators
     */
    SegmentRegistryImpl(final SegmentIdAllocator segmentIdAllocator,
            final SegmentRegistryFileSystem fileSystem,
            final SegmentRegistryCache<SegmentId, Segment<K, V>> cache,
            final BusyRetryPolicy closeRetryPolicy,
            final SegmentRegistryStateMachine gate,
            final PreparedSegmentWriterFactory<K, V> preparedSegmentWriterFactory,
            final SegmentRuntimeTuner runtimeTuner,
            final BusyRetryPolicy blockingRetryPolicy,
            final Set<SegmentId> pinnedSegments) {
        this.segmentIdAllocator = Vldtn.requireNonNull(segmentIdAllocator,
                "segmentIdAllocator");
        this.fileSystem = Vldtn.requireNonNull(fileSystem, "fileSystem");
        this.cache = Vldtn.requireNonNull(cache, "cache");
        this.closeRetryPolicy = Vldtn.requireNonNull(closeRetryPolicy,
                "closeRetryPolicy");
        this.blockingRetryPolicy = Vldtn.requireNonNull(blockingRetryPolicy,
                "blockingRetryPolicy");
        this.gate = Vldtn.requireNonNull(gate, "gate");
        this.preparedSegmentWriterFactory = Vldtn
                .requireNonNull(preparedSegmentWriterFactory,
                        "preparedSegmentWriterFactory");
        this.runtimeTuner = Vldtn.requireNonNull(runtimeTuner, "runtimeTuner");
        this.blockingFacade = new BlockingSegmentRegistryAdapter<>(this,
                this.blockingRetryPolicy);
        this.pinnedSegments = Vldtn.requireNonNull(pinnedSegments,
                "pinnedSegments");
        this.handleCache = new ConcurrentHashMap<>();
        this.materialization = new SegmentRegistryMaterializationView<>(
                this.segmentIdAllocator, this.preparedSegmentWriterFactory);
        this.runtime = new SegmentRegistryRuntimeView<>(this.runtimeTuner,
                this::loadedSegmentHandlesSnapshot);
        if (!gate.finishFreezeToReady()) {
            throw new IllegalStateException(
                    "Failed to transition registry from FREEZE to READY");
        }
    }

    @Override
    public SegmentHandle<K, V> loadSegment(final SegmentId segmentId) {
        final SegmentId validatedSegmentId = Vldtn.requireNonNull(segmentId,
                SEGMENT_ID_PARAMETER);
        final Segment<K, V> loaded = blockingFacade.loadSegment(
                validatedSegmentId);
        return toHandle(validatedSegmentId, loaded);
    }

    @Override
    public Optional<SegmentHandle<K, V>> tryGetSegment(
            final SegmentId segmentId) {
        final SegmentId validatedSegmentId = Vldtn.requireNonNull(segmentId,
                SEGMENT_ID_PARAMETER);
        return blockingFacade.tryGetSegment(validatedSegmentId)
                .map(segment -> toHandle(validatedSegmentId, segment));
    }

    @Override
    public SegmentHandle<K, V> createSegment() {
        final Segment<K, V> created = blockingFacade.createSegment();
        return toHandle(created.getId(), created);
    }

    @Override
    public void deleteSegment(final SegmentId segmentId) {
        blockingFacade.deleteSegment(segmentId);
        handleCache.remove(segmentId);
    }

    @Override
    public boolean deleteSegmentIfAvailable(final SegmentId segmentId) {
        final boolean deleted = blockingFacade.deleteSegmentIfAvailable(
                segmentId);
        if (deleted) {
            handleCache.remove(segmentId);
        }
        return deleted;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SegmentRegistryResult<SegmentId> allocateSegmentId() {
        final SegmentRegistryState state = gate.getState();
        if (state != SegmentRegistryState.READY) {
            return SegmentRegistryResult.fromStatus(resultForState(state));
        }
        try {
            final SegmentId segmentId = segmentIdAllocator.nextId();
            if (segmentId == null) {
                return SegmentRegistryResult.error();
            }
            return SegmentRegistryResult.ok(segmentId);
        } catch (final RuntimeException e) {
            return SegmentRegistryResult.error();
        }
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
    public SegmentRegistryResult<Segment<K, V>> tryLoadSegment(
            final SegmentId segmentId) {
        return loadSegmentInternal(segmentId);
    }

    /**
     * Creates and registers a new segment using a freshly allocated id.
     *
     * @return registry result containing the new segment or a status
     */
    @Override
    public SegmentRegistryResult<Segment<K, V>> tryCreateSegment() {
        final SegmentRegistryResult<SegmentId> allocated = allocateSegmentId();
        if (!allocated.isOk()) {
            return SegmentRegistryResult.fromStatus(allocated.getStatus());
        }
        if (allocated.getValue() == null) {
            return SegmentRegistryResult.error();
        }
        final SegmentId segmentId = allocated.getValue();
        fileSystem.ensureSegmentDirectory(segmentId);
        return loadSegmentInternal(segmentId);
    }

    /**
     * Returns the segment for the provided id.
     *
     * @param segmentId segment id to load
     * @return result containing the segment or a status
     */
    private SegmentRegistryResult<Segment<K, V>> loadSegmentInternal(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_PARAMETER);
        final SegmentRegistryState state = gate.getState();
        if (state != SegmentRegistryState.READY) {
            return SegmentRegistryResult.fromStatus(resultForState(state));
        }
        SegmentRegistryResult<Segment<K, V>> result = loadSegmentFromCache(
                segmentId);
        while (isClosedSegment(result)) {
            cache.invalidate(segmentId);
            result = loadSegmentFromCache(segmentId);
        }
        return result;
    }

    private SegmentRegistryResult<Segment<K, V>> loadSegmentFromCache(
            final SegmentId segmentId) {
        final Segment<K, V> segment;
        try {
            segment = cache.get(segmentId);
        } catch (final SegmentRegistryCache.EntryBusyException
                | SegmentBusyException ex) {
            return SegmentRegistryResult.busy();
        } catch (final RuntimeException ex) {
            logger.error("Failed to load segment '{}'.", segmentId, ex);
            return SegmentRegistryResult.error();
        }
        if (segment == null) {
            return SegmentRegistryResult.error();
        }
        return SegmentRegistryResult.ok(segment);
    }

    private boolean isClosedSegment(
            final SegmentRegistryResult<Segment<K, V>> result) {
        return result.isOk() && result.getValue() != null
                && result.getValue().getState() == SegmentState.CLOSED;
    }

    /**
     * Removes a segment from the registry and closes it.
     *
     * @param segmentId segment id to remove
     */
    @Override
    public SegmentRegistryResult<Void> tryDeleteSegment(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_PARAMETER);
        final SegmentRegistryState state = gate.getState();
        if (state != SegmentRegistryState.READY) {
            return SegmentRegistryResult.fromStatus(resultForState(state));
        }
        final SegmentRegistryCache.InvalidateStatus status = cache
                .invalidate(segmentId);
        if (status == SegmentRegistryCache.InvalidateStatus.BUSY) {
            return SegmentRegistryResult.busy();
        }
        try {
            fileSystem.deleteSegmentFiles(segmentId);
        } catch (final RuntimeException ex) {
            return SegmentRegistryResult.error();
        }
        return SegmentRegistryResult.ok();
    }

    /** {@inheritDoc} */
    @Override
    public SegmentRegistryCacheStats metricsSnapshot() {
        return cache.metricsSnapshot();
    }

    /** {@inheritDoc} */
    @Override
    public boolean updateCacheLimit(final int newLimit) {
        final SegmentRegistryState state = gate.getState();
        if (state != SegmentRegistryState.READY) {
            return false;
        }
        return cache.updateLimit(newLimit);
    }

    private void pinSegment(final SegmentId segmentId) {
        pinnedSegments.add(
                Vldtn.requireNonNull(segmentId, SEGMENT_ID_PARAMETER));
    }

    private void unpinSegment(final SegmentId segmentId) {
        if (segmentId == null) {
            return;
        }
        pinnedSegments.remove(segmentId);
    }

    private List<Segment<K, V>> loadedSegmentsSnapshot() {
        return cache.readyValuesSnapshot();
    }

    private List<SegmentHandle<K, V>> loadedSegmentHandlesSnapshot() {
        return loadedSegmentsSnapshot().stream().map(this::toHandle).toList();
    }

    /** {@inheritDoc} */
    @Override
    public SegmentRegistry.Materialization<K, V> materialization() {
        return materialization;
    }

    /** {@inheritDoc} */
    @Override
    public SegmentRegistry.Runtime<K, V> runtime() {
        return runtime;
    }

    private SegmentHandle<K, V> toHandle(final Segment<K, V> segment) {
        return toHandle(segment.getId(), segment);
    }

    private SegmentHandle<K, V> toHandle(final SegmentId segmentId,
            final Segment<K, V> segment) {
        return handleCache.computeIfAbsent(segmentId,
                id -> new BlockingSegmentHandle<>(id,
                        () -> blockingFacade.loadSegment(id),
                        blockingRetryPolicy, segment));
    }

    private static SegmentRegistryResultStatus resultForState(
            final SegmentRegistryState state) {
        // Contract mapping:
        // CLOSED -> CLOSED, ERROR -> ERROR, otherwise (FREEZE) -> BUSY.
        if (state == SegmentRegistryState.CLOSED) {
            return SegmentRegistryResultStatus.CLOSED;
        }
        if (state == SegmentRegistryState.ERROR) {
            return SegmentRegistryResultStatus.ERROR;
        }
        return SegmentRegistryResultStatus.BUSY;
    }

    /**
     * Closes all tracked segments.
     * <p>
     * Close is idempotent. Runtime close failures while unloading are surfaced
     * as {@link IndexException}.
     */
    @Override
    public void close() {
        SegmentRegistryState state = gate.getState();
        if (state == SegmentRegistryState.ERROR) {
            throw closeFailure(SegmentRegistryResultStatus.ERROR, null);
        }
        if (state == SegmentRegistryState.CLOSED) {
            return;
        }
        if (state == SegmentRegistryState.READY) {
            gate.tryEnterFreeze();
        }
        state = gate.getState();
        if (state == SegmentRegistryState.ERROR) {
            throw closeFailure(SegmentRegistryResultStatus.ERROR, null);
        }
        if (state == SegmentRegistryState.CLOSED) {
            return;
        }
        if (state != SegmentRegistryState.FREEZE) {
            throw closeFailure(SegmentRegistryResultStatus.BUSY, null);
        }
        try {
            awaitCacheClosed();
        } catch (final RuntimeException ex) {
            gate.fail();
            throw closeFailure(SegmentRegistryResultStatus.ERROR, ex);
        }
        handleCache.clear();
        if (!gate.finishFreezeToClosed()) {
            throw closeFailure(SegmentRegistryResultStatus.ERROR, null);
        }
    }

    private void awaitCacheClosed() {
        final long startNanos = closeRetryPolicy.startNanos();
        while (!cache.isEmpty()) {
            cache.clear();
            if (cache.isEmpty()) {
                return;
            }
            closeRetryPolicy.backoffOrThrow(startNanos, "registryClose", null);
        }
    }

    private IndexException closeFailure(
            final SegmentRegistryResultStatus status,
            final RuntimeException cause) {
        return new IndexException(
                String.format("Segment registry close failed: %s", status),
                cause);
    }

}
