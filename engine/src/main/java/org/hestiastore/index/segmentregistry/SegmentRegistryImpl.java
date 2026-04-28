package org.hestiastore.index.segmentregistry;

import java.util.Optional;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.OperationStatus;
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
    private final ConcurrentMap<SegmentId, BlockingSegment<K, V>> blockingSegments;

    /**
     * Creates a registry using prebuilt dependencies from the builder.
     *
     * @param segmentIdAllocator allocator for new segment ids
     * @param fileSystem         file system facade for segment directories/files
     * @param cache              prebuilt segment cache
     * @param closeRetryPolicy   retry policy for draining cache on close
     * @param gate               registry gate state machine shared by collaborators
     */
    SegmentRegistryImpl(final SegmentIdAllocator segmentIdAllocator,
            final SegmentRegistryFileSystem fileSystem,
            final SegmentRegistryCache<SegmentId, Segment<K, V>> cache,
            final BusyRetryPolicy closeRetryPolicy,
            final SegmentRegistryStateMachine gate,
            final PreparedSegmentWriterFactory<K, V> preparedSegmentWriterFactory,
            final SegmentRuntimeTuner runtimeTuner,
            final BusyRetryPolicy blockingRetryPolicy) {
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
        this.blockingSegments = new ConcurrentHashMap<>();
        this.materialization = new SegmentRegistryMaterializationView<>(
                this.segmentIdAllocator, this.preparedSegmentWriterFactory);
        this.runtime = new SegmentRegistryRuntimeView<>(this.runtimeTuner,
                this::loadedBlockingSegmentsSnapshot);
        if (!gate.finishFreezeToReady()) {
            throw new IllegalStateException(
                    "Failed to transition registry from FREEZE to READY");
        }
    }

    @Override
    public BlockingSegment<K, V> loadSegment(final SegmentId segmentId) {
        final SegmentId validatedSegmentId = Vldtn.requireNonNull(segmentId,
                SEGMENT_ID_PARAMETER);
        final Segment<K, V> loaded = blockingFacade.loadSegment(
                validatedSegmentId);
        return toBlockingSegment(validatedSegmentId, loaded);
    }

    @Override
    public Optional<BlockingSegment<K, V>> tryGetSegment(
            final SegmentId segmentId) {
        final SegmentId validatedSegmentId = Vldtn.requireNonNull(segmentId,
                SEGMENT_ID_PARAMETER);
        return blockingFacade.tryGetSegment(validatedSegmentId)
                .map(segment -> toBlockingSegment(validatedSegmentId, segment));
    }

    @Override
    public BlockingSegment<K, V> createSegment() {
        final Segment<K, V> created = blockingFacade.createSegment();
        return toBlockingSegment(created.getId(), created);
    }

    @Override
    public void deleteSegment(final SegmentId segmentId) {
        blockingFacade.deleteSegment(segmentId);
        blockingSegments.remove(segmentId);
    }

    @Override
    public boolean deleteSegmentIfAvailable(final SegmentId segmentId) {
        final boolean deleted = blockingFacade.deleteSegmentIfAvailable(
                segmentId);
        if (deleted) {
            blockingSegments.remove(segmentId);
        }
        return deleted;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationResult<SegmentId> allocateSegmentId() {
        final SegmentRegistryState state = gate.getState();
        if (state != SegmentRegistryState.READY) {
            return OperationResult.fromStatus(resultForState(state));
        }
        try {
            final SegmentId segmentId = segmentIdAllocator.nextId();
            if (segmentId == null) {
                return OperationResult.error();
            }
            return OperationResult.ok(segmentId);
        } catch (final RuntimeException e) {
            return OperationResult.error();
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
    public OperationResult<Segment<K, V>> tryLoadSegment(
            final SegmentId segmentId) {
        return loadSegmentInternal(segmentId);
    }

    /**
     * Creates and registers a new segment using a freshly allocated id.
     *
     * @return registry result containing the new segment or a status
     */
    @Override
    public OperationResult<Segment<K, V>> tryCreateSegment() {
        final OperationResult<SegmentId> allocated = allocateSegmentId();
        if (!allocated.isOk()) {
            return OperationResult.fromStatus(allocated.getStatus());
        }
        if (allocated.getValue() == null) {
            return OperationResult.error();
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
    private OperationResult<Segment<K, V>> loadSegmentInternal(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_PARAMETER);
        final SegmentRegistryState state = gate.getState();
        if (state != SegmentRegistryState.READY) {
            return OperationResult.fromStatus(resultForState(state));
        }
        OperationResult<Segment<K, V>> result = loadSegmentFromCache(
                segmentId);
        while (isClosedSegment(result)) {
            cache.invalidate(segmentId);
            result = loadSegmentFromCache(segmentId);
        }
        return result;
    }

    private OperationResult<Segment<K, V>> loadSegmentFromCache(
            final SegmentId segmentId) {
        final Segment<K, V> segment;
        try {
            segment = cache.get(segmentId);
        } catch (final SegmentRegistryCache.EntryBusyException
                | SegmentBusyException ex) {
            return OperationResult.busy();
        } catch (final RuntimeException ex) {
            logger.error("Failed to load segment '{}'.", segmentId, ex);
            return OperationResult.error();
        }
        if (segment == null) {
            return OperationResult.error();
        }
        return OperationResult.ok(segment);
    }

    private boolean isClosedSegment(
            final OperationResult<Segment<K, V>> result) {
        return result.isOk() && result.getValue() != null
                && result.getValue().getState() == SegmentState.CLOSED;
    }

    /**
     * Removes a segment from the registry and closes it.
     *
     * @param segmentId segment id to remove
     */
    @Override
    public OperationResult<Void> tryDeleteSegment(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_PARAMETER);
        final SegmentRegistryState state = gate.getState();
        if (state != SegmentRegistryState.READY) {
            return OperationResult.fromStatus(resultForState(state));
        }
        final SegmentRegistryCache.InvalidateStatus status = cache
                .invalidate(segmentId);
        if (status == SegmentRegistryCache.InvalidateStatus.BUSY) {
            return OperationResult.busy();
        }
        try {
            fileSystem.deleteSegmentFiles(segmentId);
        } catch (final RuntimeException ex) {
            return OperationResult.error();
        }
        return OperationResult.ok();
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

    private List<Segment<K, V>> loadedSegmentsSnapshot() {
        return cache.readyValuesSnapshot();
    }

    private List<BlockingSegment<K, V>> loadedBlockingSegmentsSnapshot() {
        return loadedSegmentsSnapshot().stream().map(this::toBlockingSegment)
                .toList();
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

    private BlockingSegment<K, V> toBlockingSegment(
            final Segment<K, V> segment) {
        return toBlockingSegment(segment.getId(), segment);
    }

    private BlockingSegment<K, V> toBlockingSegment(final SegmentId segmentId,
            final Segment<K, V> segment) {
        return blockingSegments.computeIfAbsent(segmentId,
                id -> new DefaultBlockingSegment<>(id,
                        () -> blockingFacade.loadSegment(id),
                        blockingRetryPolicy, segment));
    }

    private static OperationStatus resultForState(
            final SegmentRegistryState state) {
        // Contract mapping:
        // CLOSED -> CLOSED, ERROR -> ERROR, otherwise (FREEZE) -> BUSY.
        if (state == SegmentRegistryState.CLOSED) {
            return OperationStatus.CLOSED;
        }
        if (state == SegmentRegistryState.ERROR) {
            return OperationStatus.ERROR;
        }
        return OperationStatus.BUSY;
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
            throw closeFailure(OperationStatus.ERROR, null);
        }
        if (state == SegmentRegistryState.CLOSED) {
            return;
        }
        if (state == SegmentRegistryState.READY) {
            gate.tryEnterFreeze();
        }
        state = gate.getState();
        if (state == SegmentRegistryState.ERROR) {
            throw closeFailure(OperationStatus.ERROR, null);
        }
        if (state == SegmentRegistryState.CLOSED) {
            return;
        }
        if (state != SegmentRegistryState.FREEZE) {
            throw closeFailure(OperationStatus.BUSY, null);
        }
        try {
            awaitCacheClosed();
        } catch (final RuntimeException ex) {
            gate.fail();
            throw closeFailure(OperationStatus.ERROR, ex);
        }
        blockingSegments.clear();
        if (!gate.finishFreezeToClosed()) {
            throw closeFailure(OperationStatus.ERROR, null);
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
            final OperationStatus status,
            final RuntimeException cause) {
        return new IndexException(
                String.format("Segment registry close failed: %s", status),
                cause);
    }

}
