package org.hestiastore.index.segmentregistry;

import java.util.function.Function;
import java.util.function.Supplier;

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
 *
 * @param <K> key type
 * @param <V> value type
 */
final class DefaultBlockingSegment<K, V> implements BlockingSegment<K, V> {

    private static final Logger logger = LoggerFactory
            .getLogger(DefaultBlockingSegment.class);

    private final SegmentId segmentId;
    private final Supplier<Segment<K, V>> segmentLoader;
    private final BusyRetryPolicy retryPolicy;
    private final boolean automaticMaintenanceEnabled;
    private final Runtime runtime;

    DefaultBlockingSegment(final SegmentId segmentId,
            final Supplier<Segment<K, V>> segmentLoader,
            final BusyRetryPolicy retryPolicy,
            final boolean automaticMaintenanceEnabled) {
        this.segmentId = Vldtn.requireNonNull(segmentId, "segmentId");
        this.segmentLoader = Vldtn.requireNonNull(segmentLoader,
                "segmentLoader");
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
        return segmentLoader.get();
    }

    @Override
    public Runtime getRuntime() {
        return runtime;
    }

    @Override
    public OperationResult<V> tryGet(final K key) {
        return segmentLoader.get().get(key);
    }

    @Override
    public V get(final K key) {
        return runBlocking("get", segmentValue -> segmentValue.get(key));
    }

    @Override
    public OperationResult<Void> tryPut(final K key, final V value) {
        return segmentLoader.get().put(key, value);
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
        return segmentLoader.get().openIterator(isolation);
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
        return segmentLoader.get().flush();
    }

    @Override
    public void compact() {
        runBlocking("compact", Segment::compact);
    }

    @Override
    public OperationResult<Void> tryCompact() {
        return segmentLoader.get().compact();
    }

    @Override
    public K checkAndRepairConsistency() {
        return segmentLoader.get().checkAndRepairConsistency();
    }

    @Override
    public void invalidateIterators() {
        segmentLoader.get().invalidateIterators();
    }

    private <T> T runBlocking(final String operation,
            final Function<Segment<K, V>, OperationResult<T>> action) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final Segment<K, V> segment = segmentLoader.get();
            final OperationResult<T> result = action.apply(segment);
            if (result.getStatus() == OperationStatus.OK) {
                return result.getValue();
            }
            if (isRetryable(result.getStatus())) {
                logRetryableWriteCacheFull(operation, result.getStatus(),
                        segment);
                retryPolicy.backoffOrThrow(startNanos, operation, segmentId);
                continue;
            }
            throw operationFailure(operation, result.getStatus());
        }
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
            return segmentLoader.get().getState();
        }

        @Override
        public SegmentStats getStats() {
            return segmentLoader.get().getStats();
        }

        @Override
        public SegmentRuntimeSnapshot getRuntimeSnapshot() {
            return segmentLoader.get().getRuntimeSnapshot();
        }

        @Override
        public long getNumberOfKeys() {
            return segmentLoader.get().getNumberOfKeys();
        }

        @Override
        public int getNumberOfKeysInWriteCache() {
            return segmentLoader.get().getNumberOfKeysInWriteCache();
        }

        @Override
        public long getNumberOfKeysInCache() {
            return segmentLoader.get().getNumberOfKeysInCache();
        }

        @Override
        public long getNumberOfKeysInSegmentCache() {
            return segmentLoader.get().getNumberOfKeysInSegmentCache();
        }

        @Override
        public int getNumberOfDeltaCacheFiles() {
            return segmentLoader.get().getNumberOfDeltaCacheFiles();
        }

        @Override
        public void updateRuntimeLimits(final SegmentRuntimeLimits limits) {
            segmentLoader.get().applyRuntimeLimits(
                    Vldtn.requireNonNull(limits, "limits"));
        }
    }
}
