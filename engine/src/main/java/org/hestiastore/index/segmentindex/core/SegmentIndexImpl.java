package org.hestiastore.index.segmentindex.core;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Base implementation of the segment index that manages stable segment
 * storage, partition overlays, and background maintenance coordination.
 *
 * @param <K> key type
 * @param <V> value type
 */
public abstract class SegmentIndexImpl<K, V> extends AbstractCloseableResource
        implements IndexInternal<K, V> {

    private static final String OPERATION_DRAIN = "drain";
    static final String OPERATION_OPEN_FULL_ISOLATION_ITERATOR = "openFullIsolationIterator";
    private static final String INDEX_NAME_MDC_KEY = "index.name";
    private static final int DEFAULT_MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION = 2;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final IndexConfiguration<K, V> conf;
    protected final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final Stats stats = new Stats();
    private final IndexAsyncOperationTracker asyncOperationTracker = new IndexAsyncOperationTracker();
    private final AtomicLong compactRequestHighWaterMark = new AtomicLong();
    private final AtomicLong flushRequestHighWaterMark = new AtomicLong();
    private final AtomicLong lastAppliedWalLsn = new AtomicLong(0L);
    private final IndexStateCoordinator<K, V> stateCoordinator;
    private final SegmentIndexRuntime<K, V> runtime;
    private final IndexConsistencyCoordinator<K, V> consistencyCoordinator;
    private final IndexCloseCoordinator closeCoordinator;

    protected SegmentIndexImpl(final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final IndexExecutorRegistry executorRegistry) {
        final Directory nonNullDirectory = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        final IndexStateOpening<K, V> openingState = new IndexStateOpening<>(
                nonNullDirectory);
        this.stateCoordinator = new IndexStateCoordinator<>(openingState,
                SegmentIndexState.OPENING);
        try {
            this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                    "keyTypeDescriptor");
            this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                    "valueTypeDescriptor");
            this.conf = Vldtn.requireNonNull(conf, "conf");
            final String previousIndexName = MDC.get(INDEX_NAME_MDC_KEY);
            final boolean contextApplied = applyIndexContext(this.conf);
            try {
                Vldtn.requireNonNull(executorRegistry, "executorRegistry");
                this.runtime = SegmentIndexRuntime.open(logger, nonNullDirectory,
                        keyTypeDescriptor, valueTypeDescriptor, conf,
                        executorRegistry, stats,
                        compactRequestHighWaterMark,
                        flushRequestHighWaterMark, lastAppliedWalLsn,
                        this::getState, this::awaitSplitsIdle,
                        this::failWithError, this::onBackgroundSplitApplied);
                this.closeCoordinator = runtime.newCloseCoordinator(logger,
                        conf.getIndexName(),
                        () -> stateCoordinator.beginClose(this),
                        asyncOperationTracker::awaitAsyncOperations,
                        () -> setSegmentIndexState(SegmentIndexState.CLOSED),
                        stats::getGetCx, stats::getPutCx, stats::getDeleteCx,
                        () -> stateCoordinator.completeCloseStateTransition(
                                this));
                this.consistencyCoordinator = new IndexConsistencyCoordinator<>(
                        runtime.keyToSegmentMap(), runtime.segmentRegistry(),
                        keyTypeDescriptor,
                        runtime.recoveryCleanupCoordinator(),
                        runtime.backgroundSplitPolicyLoop());
                final IndexOpenCoordinator openCoordinator = new IndexOpenCoordinator(
                        logger, conf.getIndexName());
                openCoordinator.completeOpen(openingState.wasStaleLockRecovered(),
                        () -> runtime.recover(this::replayWalRecord),
                        runtime.recoveryCleanupCoordinator()::cleanupOrphanedSegmentDirectories,
                        () -> stateCoordinator.markReady(this),
                        () -> consistencyCoordinator
                                .runStartupConsistencyCheck(
                                        this::checkAndRepairConsistency),
                        runtime.backgroundSplitPolicyLoop()::scheduleScan);
            } finally {
                if (contextApplied) {
                    restorePreviousIndexName(previousIndexName);
                }
            }
        } catch (final RuntimeException e) {
            failWithError(e);
            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void put(final K key, final V value) {
        final long startedNanos = System.nanoTime();
        getIndexState().tryPerformOperation();
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        stats.incPutCx();

        if (valueTypeDescriptor.isTombstone(value)) {
            throw new IllegalArgumentException(String.format(
                    "Can't insert thombstone value '%s' into index", value));
        }
        final long walLsn = runtime.walCoordinator().appendPut(key, value);

        final IndexResult<Void> result = retryWhileBusy(
                () -> putBuffered(key, value), "put", false);
        if (result.getStatus() == IndexResultStatus.OK) {
            runtime.walCoordinator().recordAppliedLsn(walLsn);
            stats.recordWriteLatencyNanos(System.nanoTime() - startedNanos);
            return;
        }
        stats.recordWriteLatencyNanos(System.nanoTime() - startedNanos);
        throw newIndexException("put", null, result.getStatus());
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<Void> putAsync(final K key, final V value) {
        return runAsyncTracked(() -> {
            put(key, value);
            return null;
        });
    }

    /**
     * return segment iterator. It doesn't count with mein cache.
     * 
     * @param segmentId required segment id
     * @return
     */
    EntryIterator<K, V> openSegmentIterator(final SegmentId segmentId) {
        return openSegmentIterator(segmentId,
                SegmentIteratorIsolation.FAIL_FAST);
    }

    EntryIterator<K, V> openSegmentIterator(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Vldtn.requireNonNull(isolation, "isolation");
        return runtime.stableSegmentCoordinator().openIteratorWithRetry(segmentId,
                isolation);
    }

    /**
     * Opens a segment iterator over the provided window using
     * {@link SegmentIteratorIsolation#FAIL_FAST}.
     *
     * @param segmentWindows window selecting segments to iterate
     * @return iterator over the selected segments
     */
    @Override
    public EntryIterator<K, V> openSegmentIterator(
            SegmentWindow segmentWindows) {
        return openSegmentIterator(segmentWindows,
                SegmentIteratorIsolation.FAIL_FAST);
    }

    /**
     * Opens a segment iterator using the provided isolation level.
     *
     * @param segmentWindows window selecting segments to iterate
     * @param isolation      iterator isolation mode
     * @return entry iterator over the selected segments
     */
    public EntryIterator<K, V> openSegmentIterator(
            final SegmentWindow segmentWindows,
            final SegmentIteratorIsolation isolation) {
        final SegmentWindow resolvedWindows = segmentWindows == null
                ? SegmentWindow.unbounded()
                : segmentWindows;
        Vldtn.requireNonNull(isolation, "isolation");
        final EntryIterator<K, V> segmentIterator;
        segmentIterator = runtime.partitionReadCoordinator().openWindowIterator(
                resolvedWindows, isolation);
        if (isContextLoggingEnabled()) {
            return new EntryIteratorLoggingContext<>(segmentIterator, conf);
        }
        return segmentIterator;
    }

    /** {@inheritDoc} */
    @Override
    public void compact() {
        getIndexState().tryPerformOperation();
        runtime.maintenanceCoordinator().compact();
    }

    /** {@inheritDoc} */
    @Override
    public void compactAndWait() {
        getIndexState().tryPerformOperation();
        runtime.maintenanceCoordinator().compactAndWait();
    }

    /** {@inheritDoc} */
    @Override
    public V get(final K key) {
        final long startedNanos = System.nanoTime();
        getIndexState().tryPerformOperation();
        Vldtn.requireNonNull(key, "key");
        stats.incGetCx();

        final IndexResult<V> result = retryWhileBusy(
                () -> runtime.partitionReadCoordinator().get(key), "get", true);
        if (result.getStatus() == IndexResultStatus.OK) {
            stats.recordReadLatencyNanos(System.nanoTime() - startedNanos);
            return result.getValue();
        }
        stats.recordReadLatencyNanos(System.nanoTime() - startedNanos);
        throw newIndexException("get", null, result.getStatus());
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<V> getAsync(final K key) {
        return runAsyncTracked(() -> get(key));
    }

    /** {@inheritDoc} */
    @Override
    public void delete(final K key) {
        final long startedNanos = System.nanoTime();
        getIndexState().tryPerformOperation();
        Vldtn.requireNonNull(key, "key");
        stats.incDeleteCx();
        final long walLsn = runtime.walCoordinator().appendDelete(key);
        final IndexResult<Void> result = retryWhileBusy(
                () -> putBuffered(key, valueTypeDescriptor.getTombstone()),
                "delete", false);
        if (result.getStatus() == IndexResultStatus.OK) {
            runtime.walCoordinator().recordAppliedLsn(walLsn);
            stats.recordWriteLatencyNanos(System.nanoTime() - startedNanos);
            return;
        }
        stats.recordWriteLatencyNanos(System.nanoTime() - startedNanos);
        throw newIndexException("delete", null, result.getStatus());
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<Void> deleteAsync(final K key) {
        return runAsyncTracked(() -> {
            delete(key);
            return null;
        });
    }

    /** {@inheritDoc} */
    @Override
    public void checkAndRepairConsistency() {
        getIndexState().tryPerformOperation();
        consistencyCoordinator.checkAndRepairConsistency();
    }

    /** {@inheritDoc} */
    @Override
    protected void doClose() {
        closeCoordinator.close();
    }

    private <T> CompletionStage<T> runAsyncTracked(final Supplier<T> task) {
        return asyncOperationTracker.runAsyncTracked(task);
    }

    final void setIndexState(final IndexState<K, V> indexState) {
        stateCoordinator.setIndexState(indexState);
    }

    protected final IndexState<K, V> getIndexState() {
        return stateCoordinator.getIndexState();
    }

    /** {@inheritDoc} */
    @Override
    public SegmentIndexState getState() {
        return stateCoordinator.getState();
    }

    /** {@inheritDoc} */
    @Override
    public SegmentIndexMetricsSnapshot metricsSnapshot() {
        return runtime.metricsCollector().metricsSnapshot();
    }

    final void setSegmentIndexState(final SegmentIndexState state) {
        stateCoordinator.setSegmentIndexState(state);
    }

    final void failWithError(final Throwable failure) {
        stateCoordinator.failWithError(failure);
    }

    private void onBackgroundSplitApplied() {
        if (runtime != null) {
            runtime.backgroundSplitPolicyLoop().scheduleScanIfIdle();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void flush() {
        runtime.maintenanceCoordinator().flush();
    }

    /** {@inheritDoc} */
    @Override
    public void flushAndWait() {
        runtime.maintenanceCoordinator().flushAndWait();
    }

    private IndexResult<Void> putBuffered(final K key, final V value) {
        return runtime.partitionWriteCoordinator().putBuffered(key, value);
    }

    private void replayWalRecord(
            final WalRuntime.ReplayRecord<K, V> replayRecord) {
        final V value = replayRecord.getOperation() == WalRuntime.Operation.PUT
                ? replayRecord.getValue()
                : valueTypeDescriptor.getTombstone();
        final IndexResult<Void> result = retryWhileBusy(
                () -> putBuffered(replayRecord.getKey(), value), "walReplay",
                false);
        if (result.getStatus() != IndexResultStatus.OK) {
            throw newIndexException("walReplay", null, result.getStatus());
        }
        runtime.walCoordinator().recordAppliedLsn(replayRecord.getLsn());
    }

    private <T> IndexResult<T> retryWhileBusy(
            final Supplier<IndexResult<T>> operation, final String opName,
            final boolean retryClosed) {
        final long startNanos = runtime.retryPolicy().startNanos();
        while (true) {
            final IndexResult<T> result = operation.get();
            final IndexResultStatus status = result.getStatus();
            if (status == IndexResultStatus.BUSY
                    || (retryClosed && status == IndexResultStatus.CLOSED)) {
                runtime.retryPolicy().backoffOrThrow(startNanos, opName, null);
                continue;
            }
            return result;
        }
    }

    private IndexException newIndexException(final String operation,
            final SegmentId segmentId, final IndexResultStatus status) {
        final String target = segmentId == null ? ""
                : String.format(" on segment '%s'", segmentId);
        return new IndexException(
                String.format("Index operation '%s' failed%s: %s", operation,
                        target, status));
    }

    static IndexResult<Void> toVoidResult(
            final IndexResultStatus status) {
        if (status == IndexResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (status == IndexResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        if (status == IndexResultStatus.OK) {
            return IndexResult.ok();
        }
        return IndexResult.error();
    }

    protected void invalidateSegmentIterators() {
        runtime.invalidateSegmentIterators();
    }

    protected void awaitSplitsIdle() {
        runtime.awaitSplitsIdle(conf.getIndexBusyTimeoutMillis());
    }

    final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap() {
        return runtime.keyToSegmentMap();
    }

    final SegmentRegistry<K, V> segmentRegistry() {
        return runtime.segmentRegistry();
    }

    final WalRuntime<K, V> walRuntime() {
        return runtime.walRuntime();
    }

    final IndexStateCoordinator<K, V> stateCoordinator() {
        return stateCoordinator;
    }

    private static boolean applyIndexContext(
            final IndexConfiguration<?, ?> conf) {
        if (!Boolean.TRUE.equals(conf.isContextLoggingEnabled())) {
            return false;
        }
        final String indexName = normalizeIndexName(conf.getIndexName());
        if (indexName == null) {
            return false;
        }
        MDC.put(INDEX_NAME_MDC_KEY, indexName);
        return true;
    }

    private static String normalizeIndexName(final String indexName) {
        if (indexName == null) {
            return null;
        }
        final String normalized = indexName.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    private static void restorePreviousIndexName(
            final String previousIndexName) {
        if (previousIndexName == null) {
            MDC.remove(INDEX_NAME_MDC_KEY);
            return;
        }
        MDC.put(INDEX_NAME_MDC_KEY, previousIndexName);
    }

    private boolean isContextLoggingEnabled() {
        final Boolean enabled = conf.isContextLoggingEnabled();
        return enabled != null && enabled;
    }

    /** {@inheritDoc} */
    @Override
    public IndexControlPlane controlPlane() {
        return runtime.controlPlane();
    }

    /** {@inheritDoc} */
    @Override
    public IndexConfiguration<K, V> getConfiguration() {
        return conf;
    }

}
