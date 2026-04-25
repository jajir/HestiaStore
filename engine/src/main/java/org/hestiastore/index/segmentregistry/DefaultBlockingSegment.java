package org.hestiastore.index.segmentregistry;

import java.util.function.Function;
import java.util.function.Supplier;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segment.SegmentStats;

/**
 * Registry-backed implementation of retry-aware blocking segment operations.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class DefaultBlockingSegment<K, V> implements BlockingSegment<K, V> {

    private final SegmentId segmentId;
    private final Supplier<Segment<K, V>> segmentLoader;
    private final BusyRetryPolicy retryPolicy;
    private final Runtime runtime;
    private volatile Segment<K, V> segment;

    DefaultBlockingSegment(final SegmentId segmentId,
            final Supplier<Segment<K, V>> segmentLoader,
            final BusyRetryPolicy retryPolicy) {
        this(segmentId, segmentLoader, retryPolicy, null);
    }

    DefaultBlockingSegment(final SegmentId segmentId,
            final Supplier<Segment<K, V>> segmentLoader,
            final BusyRetryPolicy retryPolicy,
            final Segment<K, V> initialSegment) {
        this.segmentId = Vldtn.requireNonNull(segmentId, "segmentId");
        this.segmentLoader = Vldtn.requireNonNull(segmentLoader,
                "segmentLoader");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        this.segment = initialSegment;
        this.runtime = new RuntimeView();
    }

    @Override
    public SegmentId getId() {
        return segmentId;
    }

    @Override
    public Segment<K, V> getSegment() {
        return loadSegment();
    }

    @Override
    public Runtime getRuntime() {
        return runtime;
    }

    @Override
    public SegmentResult<V> tryGet(final K key) {
        return loadSegment().get(key);
    }

    @Override
    public V get(final K key) {
        return runBlocking("get", segmentValue -> segmentValue.get(key));
    }

    @Override
    public SegmentResult<Void> tryPut(final K key, final V value) {
        return loadSegment().put(key, value);
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
    public SegmentResult<EntryIterator<K, V>> tryOpenIterator(
            final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(isolation, "isolation");
        return loadSegment().openIterator(isolation);
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
    public SegmentResult<Void> tryFlush() {
        return loadSegment().flush();
    }

    @Override
    public void compact() {
        runBlocking("compact", Segment::compact);
    }

    @Override
    public SegmentResult<Void> tryCompact() {
        return loadSegment().compact();
    }

    @Override
    public K checkAndRepairConsistency() {
        return loadSegment().checkAndRepairConsistency();
    }

    @Override
    public void invalidateIterators() {
        loadSegment().invalidateIterators();
    }

    private Segment<K, V> loadSegment() {
        final Segment<K, V> loaded = segmentLoader.get();
        segment = loaded;
        return loaded;
    }

    private <T> T runBlocking(final String operation,
            final Function<Segment<K, V>, SegmentResult<T>> action) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final Segment<K, V> currentSegment = loadSegment();
            final SegmentResult<T> result = action.apply(currentSegment);
            if (result.getStatus() == SegmentResultStatus.OK) {
                return result.getValue();
            }
            if (isRetryable(result.getStatus())) {
                if (result.getStatus() == SegmentResultStatus.CLOSED
                        && segment == currentSegment) {
                    segment = null;
                }
                retryPolicy.backoffOrThrow(startNanos, operation, segmentId);
                continue;
            }
            throw operationFailure(operation, result.getStatus());
        }
    }

    private static boolean isRetryable(final SegmentResultStatus status) {
        return status == SegmentResultStatus.BUSY
                || status == SegmentResultStatus.CLOSED;
    }

    private IndexException operationFailure(final String operation,
            final SegmentResultStatus status) {
        return new IndexException(String.format(
                "Segment '%s' failed to %s: %s", segmentId, operation, status));
    }

    private final class RuntimeView implements Runtime {

        @Override
        public SegmentState getState() {
            return loadSegment().getState();
        }

        @Override
        public SegmentStats getStats() {
            return loadSegment().getStats();
        }

        @Override
        public SegmentRuntimeSnapshot getRuntimeSnapshot() {
            return loadSegment().getRuntimeSnapshot();
        }

        @Override
        public long getNumberOfKeys() {
            return loadSegment().getNumberOfKeys();
        }

        @Override
        public int getNumberOfKeysInWriteCache() {
            return loadSegment().getNumberOfKeysInWriteCache();
        }

        @Override
        public long getNumberOfKeysInCache() {
            return loadSegment().getNumberOfKeysInCache();
        }

        @Override
        public long getNumberOfKeysInSegmentCache() {
            return loadSegment().getNumberOfKeysInSegmentCache();
        }

        @Override
        public int getNumberOfDeltaCacheFiles() {
            return loadSegment().getNumberOfDeltaCacheFiles();
        }

        @Override
        public void updateRuntimeLimits(final SegmentRuntimeLimits limits) {
            loadSegment().applyRuntimeLimits(
                    Vldtn.requireNonNull(limits, "limits"));
        }
    }
}
