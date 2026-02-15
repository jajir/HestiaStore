package org.hestiastore.index.segmentregistry;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;

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
public final class SegmentRegistryImpl<K, V> implements SegmentRegistry<K, V> {

    private final SegmentRegistryCache<SegmentId, Segment<K, V>> cache;
    private final SegmentRegistryStateMachine gate;
    private final IndexRetryPolicy closeRetryPolicy;

    private final SegmentIdAllocator segmentIdAllocator;
    private final SegmentRegistryFileSystem fileSystem;

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
            final IndexRetryPolicy closeRetryPolicy,
            final SegmentRegistryStateMachine gate) {
        this.segmentIdAllocator = Vldtn.requireNonNull(segmentIdAllocator,
                "segmentIdAllocator");
        this.fileSystem = Vldtn.requireNonNull(fileSystem, "fileSystem");
        this.cache = Vldtn.requireNonNull(cache, "cache");
        this.closeRetryPolicy = Vldtn.requireNonNull(closeRetryPolicy,
                "closeRetryPolicy");
        this.gate = Vldtn.requireNonNull(gate, "gate");
        if (!gate.finishFreezeToReady()) {
            throw new IllegalStateException(
                    "Failed to transition registry from FREEZE to READY");
        }
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
    public SegmentRegistryResult<Segment<K, V>> getSegment(
            final SegmentId segmentId) {
        return loadSegment(segmentId);
    }

    /**
     * Creates and registers a new segment using a freshly allocated id.
     *
     * @return registry result containing the new segment or a status
     */
    @Override
    public SegmentRegistryResult<Segment<K, V>> createSegment() {
        final SegmentRegistryResult<SegmentId> allocated = allocateSegmentId();
        if (!allocated.isOk()) {
            return SegmentRegistryResult.fromStatus(allocated.getStatus());
        }
        if (allocated.getValue() == null) {
            return SegmentRegistryResult.error();
        }
        final SegmentId segmentId = allocated.getValue();
        fileSystem.ensureSegmentDirectory(segmentId);
        return loadSegment(segmentId);
    }

    /**
     * Returns the segment for the provided id.
     *
     * @param segmentId segment id to load
     * @return result containing the segment or a status
     */
    private SegmentRegistryResult<Segment<K, V>> loadSegment(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final SegmentRegistryState state = gate.getState();
        if (state != SegmentRegistryState.READY) {
            return SegmentRegistryResult.fromStatus(resultForState(state));
        }
        while (true) {
            final Segment<K, V> segment;
            try {
                segment = cache.get(segmentId);
            } catch (final SegmentRegistryCache.EntryBusyException
                    | SegmentBusyException ex) {
                return SegmentRegistryResult.busy();
            } catch (final RuntimeException ex) {
                return SegmentRegistryResult.error();
            }
            if (segment == null) {
                return SegmentRegistryResult.error();
            }
            if (segment.getState() == SegmentState.CLOSED) {
                cache.invalidate(segmentId);
                continue;
            }
            return SegmentRegistryResult.ok(segment);
        }
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
     * Close is idempotent. Runtime close failures while unloading are handled by
     * cache/entry semantics and may leave entries unavailable (BUSY) by design.
     */
    @Override
    public SegmentRegistryResult<Void> close() {
        SegmentRegistryState state = gate.getState();
        if (state == SegmentRegistryState.ERROR) {
            return SegmentRegistryResult.error();
        }
        if (state == SegmentRegistryState.CLOSED) {
            return SegmentRegistryResult.closed();
        }
        if (state == SegmentRegistryState.READY) {
            gate.tryEnterFreeze();
        }
        state = gate.getState();
        if (state == SegmentRegistryState.ERROR) {
            return SegmentRegistryResult.error();
        }
        if (state == SegmentRegistryState.CLOSED) {
            return SegmentRegistryResult.closed();
        }
        if (state != SegmentRegistryState.FREEZE) {
            return SegmentRegistryResult.busy();
        }
        try {
            awaitCacheClosed();
        } catch (final RuntimeException ex) {
            gate.fail();
            return SegmentRegistryResult.error();
        }
        if (!gate.finishFreezeToClosed()) {
            return SegmentRegistryResult.error();
        }
        return SegmentRegistryResult.ok();
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

}
