package org.hestiastore.index.segmentregistry;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segment.SegmentStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry-backed implementation of retry-aware blocking segment operations.
 * Uses the segment supplied during registry lookup directly and reloads only
 * after that segment has closed.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class DefaultBlockingSegment<K, V> implements BlockingSegment<K, V> {

    private static final Logger logger = LoggerFactory
            .getLogger(DefaultBlockingSegment.class);

    private final SegmentId segmentId;
    private final AtomicReference<Segment<K, V>> segment;
    private final BlockingSegmentRegistryAdapter<K, V> segmentRegistry;
    private final BusyRetryPolicy retryPolicy;
    private final boolean automaticMaintenanceEnabled;
    private final Runtime runtime;

    /**
     * Creates a blocking handle for an already loaded segment.
     *
     * @param segmentId requested segment id
     * @param segment initial loaded segment
     * @param segmentRegistry registry adapter used only to recover a closed
     *        segment
     * @param retryPolicy retry policy for transient operation outcomes
     * @param automaticMaintenanceEnabled whether full write caches are
     *        retryable
     */
    DefaultBlockingSegment(final SegmentId segmentId,
            final Segment<K, V> segment,
            final BlockingSegmentRegistryAdapter<K, V> segmentRegistry,
            final BusyRetryPolicy retryPolicy,
            final boolean automaticMaintenanceEnabled) {
        final Segment<K, V> loadedSegment = Vldtn.requireNonNull(segment,
                "segment");
        this.segmentId = Vldtn.requireNonNull(segmentId, "segmentId");
        this.segment = new AtomicReference<>(loadedSegment);
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        this.automaticMaintenanceEnabled = automaticMaintenanceEnabled;
        this.runtime = new RuntimeView();
    }

    @Override
    public SegmentId getId() {
        return segmentId;
    }

    @Override
    public Segment<K, V> getSegment() {
        return currentSegment();
    }

    @Override
    public Runtime getRuntime() {
        return runtime;
    }

    @Override
    public OperationResult<V> tryGet(final K key) {
        return currentSegment().get(key);
    }

    @Override
    public V get(final K key) {
        return runBlocking("get", segmentValue -> segmentValue.get(key));
    }

    @Override
    public OperationResult<Void> tryPut(final K key, final V value) {
        return currentSegment().put(key, value);
    }

    @Override
    public void put(final K key, final V value) {
        runBlocking("put", segmentValue -> segmentValue.put(key, value));
    }

    @Override
    public EntryIterator<K, V> openIterator() {
        return openIterator(SegmentIteratorIsolation.FAIL_FAST);
    }

    @Override
    public OperationResult<EntryIterator<K, V>> tryOpenIterator(
            final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(isolation, "isolation");
        return currentSegment().openIterator(isolation);
    }

    @Override
    public EntryIterator<K, V> openIterator(
            final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(isolation, "isolation");
        return runBlocking("openIterator",
                segmentValue -> segmentValue.openIterator(isolation));
    }

    @Override
    public void flush() {
        runBlocking("flush", Segment::flush);
    }

    @Override
    public OperationResult<Void> tryFlush() {
        return currentSegment().flush();
    }

    @Override
    public void compact() {
        runBlocking("compact", Segment::compact);
    }

    @Override
    public OperationResult<Void> tryCompact() {
        return currentSegment().compact();
    }

    @Override
    public K checkAndRepairConsistency() {
        return runBlocking("checkAndRepairConsistency",
                Segment::tryCheckAndRepairConsistency);
    }

    @Override
    public void invalidateIterators() {
        currentSegment().invalidateIterators();
    }

    /**
     * Replaces a closed segment generation with the loaded generation returned
     * by the registry. A live generation is never overwritten by a stale
     * concurrent lookup.
     *
     * @param loadedSegment loaded segment generation
     */
    void updateSegment(final Segment<K, V> loadedSegment) {
        final Segment<K, V> replacement = Vldtn.requireNonNull(loadedSegment,
                "loadedSegment");
        Segment<K, V> current = segment.get();
        while (current != replacement
                && current.getState() == SegmentState.CLOSED) {
            if (segment.compareAndSet(current, replacement)) {
                return;
            }
            current = segment.get();
        }
    }

    private <T> T runBlocking(final String operation,
            final Function<Segment<K, V>, OperationResult<T>> action) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final Segment<K, V> current = segment.get();
            final OperationResult<T> result = action.apply(current);
            if (result.getStatus() == OperationStatus.OK) {
                return result.getValue();
            }
            if (isRetryable(result.getStatus())) {
                logRetryableWriteCacheFull(operation, result.getStatus(),
                        current);
                retryPolicy.backoffOrThrow(startNanos, operation, segmentId);
                reloadClosedSegment(result.getStatus(), current);
                continue;
            }
            throw operationFailure(operation, result.getStatus());
        }
    }

    private Segment<K, V> currentSegment() {
        final Segment<K, V> current = segment.get();
        if (current.getState() != SegmentState.CLOSED) {
            return current;
        }
        reloadClosedSegment(OperationStatus.CLOSED, current);
        return segment.get();
    }

    private void reloadClosedSegment(final OperationStatus status,
            final Segment<K, V> closedSegment) {
        if (status != OperationStatus.CLOSED
                || segment.get() != closedSegment) {
            return;
        }
        final Segment<K, V> loaded = segmentRegistry.loadSegment(segmentId);
        segment.compareAndSet(closedSegment, loaded);
    }

    private boolean isRetryable(final OperationStatus status) {
        return status == OperationStatus.BUSY
                || status == OperationStatus.CLOSED
                || (status == OperationStatus.WRITE_CACHE_FULL
                        && automaticMaintenanceEnabled);
    }

    private void logRetryableWriteCacheFull(final String operation,
            final OperationStatus status, final Segment<K, V> segment) {
        if (status != OperationStatus.WRITE_CACHE_FULL
                || !logger.isDebugEnabled()) {
            return;
        }
        logger.debug(
                "Write cache full treated as busy: segment='{}' operation='{}' automaticMaintenanceEnabled='{}' state='{}' writeCacheKeys='{}'",
                segmentId, operation, automaticMaintenanceEnabled,
                segment.getState(), segment.getNumberOfKeysInWriteCache());
    }

    private IndexException operationFailure(final String operation,
            final OperationStatus status) {
        if (status == OperationStatus.WRITE_CACHE_FULL) {
            return new IndexException(String.format(
                    "Write cache is full for segment '%s' and automatic maintenance is disabled.",
                    segmentId));
        }
        return new IndexException(String.format(
                "Segment '%s' failed to %s: %s", segmentId, operation, status));
    }

    private final class RuntimeView implements Runtime {

        @Override
        public SegmentState getState() {
            return currentSegment().getState();
        }

        @Override
        public SegmentStats getStats() {
            return currentSegment().getStats();
        }

        @Override
        public SegmentRuntimeSnapshot getRuntimeSnapshot() {
            return currentSegment().getRuntimeSnapshot();
        }

        @Override
        public long getNumberOfKeys() {
            return currentSegment().getNumberOfKeys();
        }

        @Override
        public int getNumberOfKeysInWriteCache() {
            return currentSegment().getNumberOfKeysInWriteCache();
        }

        @Override
        public long getNumberOfKeysInCache() {
            return currentSegment().getNumberOfKeysInCache();
        }

        @Override
        public long getNumberOfKeysInSegmentCache() {
            return currentSegment().getNumberOfKeysInSegmentCache();
        }

        @Override
        public int getNumberOfDeltaCacheFiles() {
            return currentSegment().getNumberOfDeltaCacheFiles();
        }

        @Override
        public void updateRuntimeLimits(final SegmentRuntimeLimits limits) {
            currentSegment().applyRuntimeLimits(
                    Vldtn.requireNonNull(limits, "limits"));
        }
    }
}
