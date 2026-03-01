package org.hestiastore.index.segmentindex.core;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.F;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexConfigurationManagement;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.control.IndexRuntimeView;
import org.hestiastore.index.control.model.ConfigurationSnapshot;
import org.hestiastore.index.control.model.IndexRuntimeSnapshot;
import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimePatchValidation;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.control.model.ValidationIssue;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.split.SegmentAsyncSplitCoordinator;
import org.hestiastore.index.segmentindex.split.SegmentSplitCoordinator;
import org.hestiastore.index.segmentindex.split.SegmentWriterTxFactory;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentFactory;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base implementation of the segment index that manages segments, mappings, and
 * maintenance coordination.
 *
 * @param <K> key type
 * @param <V> value type
 */
public abstract class SegmentIndexImpl<K, V> extends AbstractCloseableResource
        implements IndexInternal<K, V> {

    private static final String OPERATION_COMPACT = "compact";
    private static final String OPERATION_FLUSH = "flush";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final IndexConfiguration<K, V> conf;
    protected final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentFactory<K, V> segmentFactory;
    private final SegmentMaintenanceCoordinator<K, V> maintenanceCoordinator;
    private final SegmentIndexCore<K, V> core;
    private final IndexRetryPolicy retryPolicy;
    private final RuntimeTuningState runtimeTuningState;
    private final IndexControlPlane controlPlane;
    private final WalRuntime<K, V> walRuntime;
    private final Stats stats = new Stats();
    private final AtomicLong compactRequestHighWaterMark = new AtomicLong();
    private final AtomicLong flushRequestHighWaterMark = new AtomicLong();
    private final Object asyncMonitor = new Object();
    private int asyncInFlight = 0;
    private final ThreadLocal<Boolean> inAsyncOperation = ThreadLocal
            .withInitial(() -> Boolean.FALSE);
    private final AtomicLong lastAppliedWalLsn = new AtomicLong(0L);
    private volatile IndexState<K, V> indexState;
    private volatile SegmentIndexState segmentIndexState = SegmentIndexState.OPENING;

    protected SegmentIndexImpl(final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final IndexExecutorRegistry executorRegistry) {
        Vldtn.requireNonNull(directoryFacade, "directoryFacade");
        final IndexStateOpening<K, V> openingState = new IndexStateOpening<>(
                directoryFacade);
        setIndexState(openingState);
        setSegmentIndexState(SegmentIndexState.OPENING);
        try {
            this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                    "keyTypeDescriptor");
            this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                    "valueTypeDescriptor");
            this.conf = Vldtn.requireNonNull(conf, "conf");
            Vldtn.requireNonNull(executorRegistry, "executorRegistry");
            this.runtimeTuningState = RuntimeTuningState
                    .fromConfiguration(conf);
            final KeyToSegmentMap<K> keyToSegmentMapDelegate = new KeyToSegmentMap<>(
                    directoryFacade, keyTypeDescriptor);
            this.keyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                    keyToSegmentMapDelegate);
            this.segmentFactory = new SegmentFactory<>(directoryFacade,
                    keyTypeDescriptor, valueTypeDescriptor, conf,
                    executorRegistry.getSegmentMaintenanceExecutor());
            this.segmentRegistry = SegmentRegistry.<K, V>builder()
                    .withDirectoryFacade(directoryFacade)
                    .withKeyTypeDescriptor(keyTypeDescriptor)
                    .withValueTypeDescriptor(valueTypeDescriptor)
                    .withConfiguration(conf)
                    .withSegmentMaintenanceExecutor(
                            executorRegistry.getSegmentMaintenanceExecutor())
                    .withRegistryMaintenanceExecutor(
                            executorRegistry.getRegistryMaintenanceExecutor())
                    .build();
            final SegmentWriterTxFactory<K, V> writerTxFactory = id -> segmentFactory
                    .newSegmentBuilder(id).openWriterTx();
            final SegmentSplitCoordinator<K, V> splitCoordinator = new SegmentSplitCoordinator<>(
                    conf, keyToSegmentMap, segmentRegistry, writerTxFactory);
            final SegmentAsyncSplitCoordinator<K, V> asyncSplitCoordinator = new SegmentAsyncSplitCoordinator<>(
                    splitCoordinator,
                    executorRegistry.getIndexMaintenanceExecutor());
            this.maintenanceCoordinator = new SegmentMaintenanceCoordinator<>(
                    conf, keyToSegmentMap, asyncSplitCoordinator);
            this.core = new SegmentIndexCore<>(keyToSegmentMap, segmentRegistry,
                    maintenanceCoordinator);
            this.retryPolicy = new IndexRetryPolicy(
                    conf.getIndexBusyBackoffMillis(),
                    conf.getIndexBusyTimeoutMillis());
            this.controlPlane = new IndexControlPlaneImpl();
            this.walRuntime = WalRuntime.open(directoryFacade, conf.getWal(),
                    keyTypeDescriptor, valueTypeDescriptor);
            if (walRuntime.isEnabled()) {
                final WalRuntime.RecoveryResult recoveryResult = walRuntime
                        .recover(this::replayWalRecord);
                if (recoveryResult.maxLsn() > 0L) {
                    lastAppliedWalLsn
                            .set(Math.max(lastAppliedWalLsn.get(),
                                    recoveryResult.maxLsn()));
                }
            }
            getIndexState().onReady(this);
            setSegmentIndexState(SegmentIndexState.READY);
            if (openingState.wasStaleLockRecovered()) {
                logger.info(
                        "Recovered stale index lock (.lock). Index is going to be checked for consistency and unlocked.");
                checkAndRepairConsistency();
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
        final long walLsn = appendWalPutWithFailureHandling(key, value);

        final IndexResult<Void> result = retryWhileBusyWithSplitWait(
                () -> core.put(key, value), "put", key, true);
        if (result.getStatus() == IndexResultStatus.OK) {
            recordAppliedWalLsn(walLsn);
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
        return openIteratorWithRetry(segmentId, isolation);
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
        if (isolation == SegmentIteratorIsolation.FULL_ISOLATION) {
            awaitSplitsIdle();
        }
        final EntryIterator<K, V> segmentIterator = new SegmentsIterator<>(
                keyToSegmentMap.getSegmentIds(resolvedWindows), segmentRegistry,
                isolation, retryPolicy);
        if (isContextLoggingEnabled()) {
            return new EntryIteratorLoggingContext<>(segmentIterator, conf);
        }
        return segmentIterator;
    }

    /** {@inheritDoc} */
    @Override
    public void compact() {
        getIndexState().tryPerformOperation();
        keyToSegmentMap.getSegmentIds()
                .forEach(segmentId -> compactSegment(segmentId, false));
    }

    /** {@inheritDoc} */
    @Override
    public void compactAndWait() {
        getIndexState().tryPerformOperation();
        keyToSegmentMap.getSegmentIds()
                .forEach(segmentId -> compactSegment(segmentId, true));
    }

    /** {@inheritDoc} */
    @Override
    public V get(final K key) {
        final long startedNanos = System.nanoTime();
        getIndexState().tryPerformOperation();
        Vldtn.requireNonNull(key, "key");
        stats.incGetCx();

        final IndexResult<V> result = retryWhileBusyWithSplitWait(
                () -> core.get(key), "get", key, true);
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
        final long walLsn = appendWalDeleteWithFailureHandling(key);
        final IndexResult<Void> result = retryWhileBusyWithSplitWait(
                () -> core.put(key, valueTypeDescriptor.getTombstone()),
                "delete", key, true);
        if (result.getStatus() == IndexResultStatus.OK) {
            recordAppliedWalLsn(walLsn);
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
        keyToSegmentMap.checkUniqueSegmentIds();
        final IndexConsistencyChecker<K, V> checker = new IndexConsistencyChecker<>(
                keyToSegmentMap, segmentRegistry, keyTypeDescriptor);
        checker.checkAndRepairConsistency();
    }

    /** {@inheritDoc} */
    @Override
    protected void doClose() {
        try {
            getIndexState().onClose(this);
            setSegmentIndexState(SegmentIndexState.CLOSED);
            awaitAsyncOperations();
            flushSegments(true);
            final SegmentRegistryResult<Void> closeResult = segmentRegistry
                    .close();
            if (closeResult.getStatus() != SegmentRegistryResultStatus.OK
                    && closeResult
                            .getStatus() != SegmentRegistryResultStatus.CLOSED) {
                throw new IndexException(
                        String.format("Index operation '%s' failed: %s", "close",
                                closeResult.getStatus()));
            }
            keyToSegmentMap.optionalyFlush();
            checkpointWal();
            if (logger.isDebugEnabled()) {
                logger.debug(String.format(
                        "Index is closing, where was %s gets, %s puts and %s deletes.",
                        F.fmt(stats.getGetCx()), F.fmt(stats.getPutCx()),
                        F.fmt(stats.getDeleteCx())));
            }
        } finally {
            walRuntime.close();
        }
    }

    private <T> CompletionStage<T> runAsyncTracked(final Supplier<T> task) {
        incrementAsync();
        try {
            return CompletableFuture.supplyAsync(() -> {
                final boolean previous = inAsyncOperation.get();
                inAsyncOperation.set(Boolean.TRUE);
                try {
                    return task.get();
                } finally {
                    inAsyncOperation.set(previous);
                    decrementAsync();
                }
            });
        } catch (final RuntimeException e) {
            decrementAsync();
            throw e;
        }
    }

    private void incrementAsync() {
        synchronized (asyncMonitor) {
            asyncInFlight++;
        }
    }

    private void decrementAsync() {
        synchronized (asyncMonitor) {
            asyncInFlight--;
            asyncMonitor.notifyAll();
        }
    }

    private void awaitAsyncOperations() {
        if (inAsyncOperation.get()) {
            throw new IllegalStateException(
                    "close() must not be called from an async index operation.");
        }
        synchronized (asyncMonitor) {
            while (asyncInFlight > 0) {
                try {
                    asyncMonitor.wait();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "Interrupted while waiting for async operations to finish.",
                            e);
                }
            }
        }
    }

    final void setIndexState(final IndexState<K, V> indexState) {
        this.indexState = Vldtn.requireNonNull(indexState, "indexState");
    }

    protected final IndexState<K, V> getIndexState() {
        return indexState;
    }

    /** {@inheritDoc} */
    @Override
    public SegmentIndexState getState() {
        return segmentIndexState;
    }

    /** {@inheritDoc} */
    @Override
    public SegmentIndexMetricsSnapshot metricsSnapshot() {
        final SegmentRegistryCacheStats cacheStats = segmentRegistry
                .metricsSnapshot();
        final SegmentRuntimeAggregate segmentRuntime = collectSegmentRuntime();
        final var walStats = walRuntime.statsSnapshot();
        final long compactRequestCount = Math.max(stats.getCompactRequestCx(),
                updateHighWaterMark(compactRequestHighWaterMark,
                        segmentRuntime.compactRequestCount));
        final long flushRequestCount = Math.max(stats.getFlushRequestCx(),
                updateHighWaterMark(flushRequestHighWaterMark,
                        segmentRuntime.flushRequestCount));
        return new SegmentIndexMetricsSnapshot(stats.getGetCx(),
                stats.getPutCx(), stats.getDeleteCx(), cacheStats.hitCount(),
                cacheStats.missCount(), cacheStats.loadCount(),
                cacheStats.evictionCount(), cacheStats.size(),
                cacheStats.limit(),
                runtimeTuningState.effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE),
                runtimeTuningState.effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE),
                runtimeTuningState.effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE),
                segmentRuntime.segmentCount, segmentRuntime.segmentReadyCount,
                segmentRuntime.segmentMaintenanceCount,
                segmentRuntime.segmentErrorCount,
                segmentRuntime.segmentClosedCount,
                segmentRuntime.segmentBusyCount,
                segmentRuntime.totalSegmentKeys,
                segmentRuntime.totalSegmentCacheKeys,
                segmentRuntime.totalWriteCacheKeys,
                segmentRuntime.totalDeltaCacheFiles, compactRequestCount,
                flushRequestCount, maintenanceCoordinator.splitScheduledCount(),
                maintenanceCoordinator.splitInFlightCount(), 0, 0, 0, 0,
                stats.getReadLatencyP50Micros(),
                stats.getReadLatencyP95Micros(),
                stats.getReadLatencyP99Micros(),
                stats.getWriteLatencyP50Micros(),
                stats.getWriteLatencyP95Micros(),
                stats.getWriteLatencyP99Micros(),
                conf.getBloomFilterNumberOfHashFunctions(),
                conf.getBloomFilterIndexSizeInBytes(),
                conf.getBloomFilterProbabilityOfFalsePositive(),
                segmentRuntime.bloomFilterRequestCount,
                segmentRuntime.bloomFilterRefusedCount,
                segmentRuntime.bloomFilterPositiveCount,
                segmentRuntime.bloomFilterFalsePositiveCount, walRuntime.isEnabled(),
                walStats.appendCount(), walStats.appendBytes(),
                walStats.syncCount(), walStats.syncFailureCount(),
                walStats.corruptionCount(), walStats.truncationCount(),
                walStats.retainedBytes(), walStats.segmentCount(),
                walStats.durableLsn(), walStats.checkpointLsn(),
                walStats.pendingSyncBytes(),
                segmentRuntime.segmentRuntimeSnapshots, getState());
    }

    final void setSegmentIndexState(final SegmentIndexState state) {
        this.segmentIndexState = Vldtn.requireNonNull(state, "state");
    }

    final void failWithError(final Throwable failure) {
        setSegmentIndexState(SegmentIndexState.ERROR);
        FileLock fileLock = null;
        final IndexState<K, V> currentState = indexState;
        if (currentState instanceof IndexStateReady<?, ?> readyState) {
            fileLock = readyState.getFileLock();
        } else if (currentState instanceof IndexStateOpening<?, ?> openingState) {
            fileLock = openingState.getFileLock();
        }
        setIndexState(new IndexStateError<>(failure, fileLock));
    }

    /** {@inheritDoc} */
    @Override
    public void flush() {
        flushSegments(false);
        keyToSegmentMap.optionalyFlush();
    }

    /** {@inheritDoc} */
    @Override
    public void flushAndWait() {
        flushSegments(true);
        keyToSegmentMap.optionalyFlush();
        checkpointWal();
    }

    private void replayWalRecord(final WalRuntime.ReplayRecord<K, V> record) {
        final V value = record.getOperation() == WalRuntime.Operation.PUT
                ? record.getValue()
                : valueTypeDescriptor.getTombstone();
        final IndexResult<Void> result = retryWhileBusyWithSplitWait(
                () -> core.put(record.getKey(), value), "walReplay",
                record.getKey(), true);
        if (result.getStatus() != IndexResultStatus.OK) {
            throw newIndexException("walReplay", null, result.getStatus());
        }
        recordAppliedWalLsn(record.getLsn());
    }

    private void recordAppliedWalLsn(final long walLsn) {
        if (walLsn <= 0L || !walRuntime.isEnabled()) {
            return;
        }
        while (true) {
            final long current = lastAppliedWalLsn.get();
            if (walLsn <= current) {
                return;
            }
            if (lastAppliedWalLsn.compareAndSet(current, walLsn)) {
                return;
            }
        }
    }

    private void checkpointWal() {
        if (!walRuntime.isEnabled()) {
            return;
        }
        try {
            walRuntime.onCheckpoint(lastAppliedWalLsn.get());
            if (logger.isDebugEnabled()) {
                final var walStats = walRuntime.statsSnapshot();
                logger.debug(
                        "WAL checkpoint: durableLsn={}, checkpointLsn={}, retainedBytes={}, segments={}",
                        walStats.durableLsn(), walStats.checkpointLsn(),
                        walStats.retainedBytes(), walStats.segmentCount());
            }
        } catch (final RuntimeException failure) {
            handleWalRuntimeFailure(failure);
            throw failure;
        }
    }

    private void enforceWalRetentionPressureIfNeeded() {
        if (!walRuntime.isEnabled() || !walRuntime.isRetentionPressure()) {
            return;
        }
        logger.warn(
                "WAL retention pressure detected: retainedBytes={} threshold={}. Starting forced checkpoint/backpressure.",
                walRuntime.retainedBytes(),
                conf.getWal().getMaxBytesBeforeForcedCheckpoint());
        final long startNanos = retryPolicy.startNanos();
        while (walRuntime.isRetentionPressure()) {
            flushSegments(true);
            keyToSegmentMap.optionalyFlush();
            checkpointWal();
            if (!walRuntime.isRetentionPressure()) {
                return;
            }
            retryPolicy.backoffOrThrow(startNanos, "walBackpressure", null);
        }
    }

    private long appendWalPutWithFailureHandling(final K key, final V value) {
        if (!walRuntime.isEnabled()) {
            return 0L;
        }
        try {
            enforceWalRetentionPressureIfNeeded();
            return walRuntime.appendPut(key, value);
        } catch (final RuntimeException failure) {
            handleWalRuntimeFailure(failure);
            throw failure;
        }
    }

    private long appendWalDeleteWithFailureHandling(final K key) {
        if (!walRuntime.isEnabled()) {
            return 0L;
        }
        try {
            enforceWalRetentionPressureIfNeeded();
            return walRuntime.appendDelete(key);
        } catch (final RuntimeException failure) {
            handleWalRuntimeFailure(failure);
            throw failure;
        }
    }

    private void handleWalRuntimeFailure(final RuntimeException failure) {
        if (!walRuntime.isEnabled() || !walRuntime.hasSyncFailure()) {
            return;
        }
        final SegmentIndexState state = getState();
        if (state == SegmentIndexState.CLOSED
                || state == SegmentIndexState.ERROR) {
            return;
        }
        logger.error(
                "WAL sync failure detected. Transitioning index to ERROR state.",
                failure);
        failWithError(failure);
    }

    private void flushSegments(final boolean waitForCompletion) {
        keyToSegmentMap.getSegmentIds().forEach(
                segmentId -> flushSegment(segmentId, waitForCompletion));
    }

    private void compactSegment(final SegmentId segmentId,
            final boolean waitForCompletion) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final IndexResult<Segment<K, V>> result = core.compact(segmentId);
            final IndexResultStatus status = result.getStatus();
            if (status == IndexResultStatus.OK) {
                if (waitForCompletion) {
                    final Segment<K, V> segment = result.getValue();
                    if (segment != null) {
                        awaitSegmentReady(segmentId, OPERATION_COMPACT,
                                segment);
                    }
                }
                return;
            }
            if (status == IndexResultStatus.CLOSED) {
                return;
            }
            if (status == IndexResultStatus.BUSY) {
                if (!isSegmentStillMapped(segmentId)) {
                    return;
                }
                retryPolicy.backoffOrThrow(startNanos, OPERATION_COMPACT,
                        segmentId);
                continue;
            }
            if (status == IndexResultStatus.ERROR
                    && !isSegmentStillMapped(segmentId)) {
                return;
            }
            throw newIndexException(OPERATION_COMPACT, segmentId, status);
        }
    }

    private void flushSegment(final SegmentId segmentId,
            final boolean waitForCompletion) {
        stats.incFlushRequestCx();
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final IndexResult<Segment<K, V>> result = core.flush(segmentId);
            final IndexResultStatus status = result.getStatus();
            if (status == IndexResultStatus.OK) {
                if (waitForCompletion) {
                    final Segment<K, V> segment = result.getValue();
                    if (segment != null) {
                        awaitSegmentReady(segmentId, OPERATION_FLUSH, segment);
                    }
                }
                return;
            }
            if (status == IndexResultStatus.CLOSED) {
                return;
            }
            if (status == IndexResultStatus.BUSY) {
                if (!isSegmentStillMapped(segmentId)) {
                    return;
                }
                retryPolicy.backoffOrThrow(startNanos, OPERATION_FLUSH,
                        segmentId);
                continue;
            }
            if (status == IndexResultStatus.ERROR
                    && !isSegmentStillMapped(segmentId)) {
                return;
            }
            throw newIndexException(OPERATION_FLUSH, segmentId, status);
        }
    }

    private void awaitSegmentReady(final SegmentId segmentId,
            final String operation, final Segment<K, V> segment) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentState state = segment.getState();
            if (state == SegmentState.READY || state == SegmentState.CLOSED) {
                return;
            }
            if (state == SegmentState.ERROR) {
                throw new IndexException(
                        String.format("Segment '%s' failed during %s.",
                                segmentId, operation));
            }
            retryPolicy.backoffOrThrow(startNanos, operation, segmentId);
        }
    }

    private EntryIterator<K, V> openIteratorWithRetry(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final IndexResult<EntryIterator<K, V>> result = core
                    .openIterator(segmentId, isolation);
            if (result.getStatus() == IndexResultStatus.OK) {
                return result.getValue();
            }
            if (result.getStatus() == IndexResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "openIterator",
                        segmentId);
                continue;
            }
            throw newIndexException("openIterator", segmentId,
                    result.getStatus());
        }
    }

    private <T> IndexResult<T> retryWhileBusyWithSplitWait(
            final Supplier<IndexResult<T>> operation, final String opName,
            final K key, final boolean retryClosed) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final IndexResult<T> result = operation.get();
            final IndexResultStatus status = result.getStatus();
            if (status == IndexResultStatus.BUSY
                    || (retryClosed && status == IndexResultStatus.CLOSED)) {
                awaitSplitCompletionForKey(key, startNanos);
                retryPolicy.backoffOrThrow(startNanos, opName, null);
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

    protected void invalidateSegmentIterators() {
        keyToSegmentMap.getSegmentIds().forEach(segmentId -> {
            final SegmentRegistryResult<Segment<K, V>> loaded = segmentRegistry
                    .getSegment(segmentId);
            if (loaded.getStatus() == SegmentRegistryResultStatus.OK
                    && loaded.getValue() != null) {
                loaded.getValue().invalidateIterators();
                return;
            }
            if (!isSegmentStillMapped(segmentId)) {
                return;
            }
            if (loaded.getStatus() == SegmentRegistryResultStatus.BUSY) {
                logger.debug(
                        "Skipping iterator invalidation for segment '{}' because it is BUSY.",
                        segmentId);
                return;
            }
            logger.debug(
                    "Skipping iterator invalidation for segment '{}' because registry returned status '{}'.",
                    segmentId, loaded.getStatus());
        });
    }

    protected void awaitSplitsIdle() {
        maintenanceCoordinator
                .awaitSplitsIdle(conf.getIndexBusyTimeoutMillis());
    }

    private void awaitSplitCompletionForKey(final K key,
            final long startNanos) {
        if (key == null) {
            return;
        }
        final long remainingMillis = remainingBusyTimeoutMillis(startNanos);
        if (remainingMillis <= 0) {
            return;
        }
        final SegmentId segmentId = keyToSegmentMap.findSegmentId(key);
        if (segmentId == null) {
            return;
        }
        maintenanceCoordinator.awaitSplitCompletionIfInFlight(segmentId,
                remainingMillis);
    }

    private long remainingBusyTimeoutMillis(final long startNanos) {
        final long timeoutMillis = conf.getIndexBusyTimeoutMillis();
        final long elapsedNanos = System.nanoTime() - startNanos;
        final long remainingNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
                - elapsedNanos;
        if (remainingNanos <= 0) {
            return 0;
        }
        return TimeUnit.NANOSECONDS.toMillis(remainingNanos);
    }

    private boolean isSegmentStillMapped(final SegmentId segmentId) {
        return keyToSegmentMap.getSegmentIds().contains(segmentId);
    }

    private boolean isContextLoggingEnabled() {
        final Boolean enabled = conf.isContextLoggingEnabled();
        return enabled != null && enabled;
    }

    private static long updateHighWaterMark(final AtomicLong highWaterMark,
            final long observedValue) {
        final long sanitizedValue = Math.max(0L, observedValue);
        while (true) {
            final long currentValue = highWaterMark.get();
            if (sanitizedValue <= currentValue) {
                return currentValue;
            }
            if (highWaterMark.compareAndSet(currentValue, sanitizedValue)) {
                return sanitizedValue;
            }
        }
    }

    private SegmentRuntimeAggregate collectSegmentRuntime() {
        final SegmentRuntimeAggregate aggregate = new SegmentRuntimeAggregate();
        final List<SegmentId> mappedSegmentIds = keyToSegmentMap
                .getSegmentIds();
        aggregate.segmentCount = mappedSegmentIds.size();
        if (mappedSegmentIds.isEmpty()) {
            return aggregate;
        }
        final Set<SegmentId> mappedSegmentIdSet = new HashSet<>(
                mappedSegmentIds);
        int accountedSegments = 0;
        for (final Segment<K, V> segment : segmentRegistry
                .loadedSegmentsSnapshot()) {
            if (segment != null) {
                final SegmentRuntimeSnapshot segmentRuntime = segment
                        .getRuntimeSnapshot();
                final SegmentId segmentId = segmentRuntime.getSegmentId();
                if (mappedSegmentIdSet.contains(segmentId)) {
                    accountedSegments++;
                    final SegmentState state = segmentRuntime.getState();
                    if (state == SegmentState.READY) {
                        aggregate.segmentReadyCount++;
                    } else if (state == SegmentState.MAINTENANCE_RUNNING
                            || state == SegmentState.FREEZE) {
                        aggregate.segmentMaintenanceCount++;
                    } else if (state == SegmentState.ERROR) {
                        aggregate.segmentErrorCount++;
                    } else if (state == SegmentState.CLOSED) {
                        aggregate.segmentClosedCount++;
                    }
                    aggregate.totalSegmentKeys += Math.max(0L,
                            segmentRuntime.getNumberOfKeys());
                    aggregate.totalSegmentCacheKeys += Math.max(0L,
                            segmentRuntime.getNumberOfKeysInSegmentCache());
                    aggregate.totalWriteCacheKeys += Math.max(0L,
                            segmentRuntime.getNumberOfKeysInWriteCache());
                    aggregate.totalDeltaCacheFiles += Math.max(0,
                            segmentRuntime.getNumberOfDeltaCacheFiles());
                    aggregate.segmentRuntimeSnapshots.add(
                            new SegmentIndexMetricsSnapshot.SegmentMetricsSnapshot(
                                    segmentRuntime));
                    aggregate.compactRequestCount += Math.max(0L,
                            segmentRuntime.getNumberOfCompacts());
                    aggregate.flushRequestCount += Math.max(0L,
                            segmentRuntime.getNumberOfFlushes());
                    aggregate.bloomFilterRequestCount += Math.max(0L,
                            segmentRuntime.getBloomFilterRequestCount());
                    aggregate.bloomFilterRefusedCount += Math.max(0L,
                            segmentRuntime.getBloomFilterRefusedCount());
                    aggregate.bloomFilterPositiveCount += Math.max(0L,
                            segmentRuntime.getBloomFilterPositiveCount());
                    aggregate.bloomFilterFalsePositiveCount += Math.max(0L,
                            segmentRuntime.getBloomFilterFalsePositiveCount());
                }
            }
        }
        aggregate.segmentBusyCount = Math.max(0,
                aggregate.segmentCount - accountedSegments);
        return aggregate;
    }

    /** {@inheritDoc} */
    @Override
    public IndexControlPlane controlPlane() {
        return controlPlane;
    }

    private RuntimePatchValidation validateRuntimePatch(
            final RuntimeConfigPatch patch) {
        final List<ValidationIssue> issues = new ArrayList<>();
        final EnumMap<RuntimeSettingKey, Integer> normalized = new EnumMap<>(
                RuntimeSettingKey.class);
        if (patch == null) {
            issues.add(new ValidationIssue(null, "patch must not be null"));
            return new RuntimePatchValidation(false, issues, normalized);
        }
        if (patch.expectedRevision() != null && patch.expectedRevision()
                .longValue() != runtimeTuningState.revision()) {
            issues.add(new ValidationIssue(null,
                    "expectedRevision does not match current revision"));
        }
        for (final Map.Entry<RuntimeSettingKey, Integer> entry : patch.values()
                .entrySet()) {
            final RuntimeSettingKey key = entry.getKey();
            final int value = entry.getValue().intValue();
            if (value < 1) {
                issues.add(new ValidationIssue(key, "value must be >= 1"));
            } else if (key == RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE
                    && value < 3) {
                issues.add(new ValidationIssue(key, "value must be >= 3"));
            } else {
                normalized.put(key, Integer.valueOf(value));
            }
        }
        final Map<RuntimeSettingKey, Integer> effective = runtimeTuningState
                .previewEffective(normalized);
        final int writeCache = effective.get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE)
                .intValue();
        final int writeCacheDuringMaintenance = effective.get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE)
                .intValue();
        if (writeCacheDuringMaintenance <= writeCache) {
            issues.add(new ValidationIssue(
                    RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE,
                    "value must be greater than maxNumberOfKeysInSegmentWriteCache"));
        }
        return new RuntimePatchValidation(issues.isEmpty(), issues, normalized);
    }

    private RuntimePatchResult applyRuntimePatch(
            final RuntimeConfigPatch patch) {
        final RuntimePatchValidation validation = validateRuntimePatch(patch);
        if (!validation.valid()) {
            return new RuntimePatchResult(false, validation,
                    runtimeTuningState.snapshotCurrent());
        }
        if (patch.dryRun()) {
            return new RuntimePatchResult(false, validation,
                    runtimeTuningState.snapshotCurrent());
        }
        final Map<RuntimeSettingKey, Integer> effective = runtimeTuningState
                .previewEffective(validation.normalizedValues());
        applyRuntimeEffectiveLimits(effective);
        final ConfigurationSnapshot snapshot = runtimeTuningState
                .apply(validation.normalizedValues());
        return new RuntimePatchResult(true, validation, snapshot);
    }

    private void applyRuntimeEffectiveLimits(
            final Map<RuntimeSettingKey, Integer> effective) {
        final int maxSegmentsInCache = effective
                .get(RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE)
                .intValue();
        segmentRegistry.updateCacheLimit(maxSegmentsInCache);
        final int maxSegmentCache = effective
                .get(RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE)
                .intValue();
        final int maxWriteCache = effective.get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE)
                .intValue();
        final int maxWriteCacheDuringMaintenance = effective.get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE)
                .intValue();
        segmentFactory.updateRuntimeLimits(maxSegmentCache, maxWriteCache,
                maxWriteCacheDuringMaintenance);
        final SegmentRuntimeLimits limits = new SegmentRuntimeLimits(
                maxSegmentCache, maxWriteCache, maxWriteCacheDuringMaintenance);
        for (final Segment<K, V> segment : segmentRegistry
                .loadedSegmentsSnapshot()) {
            segment.applyRuntimeLimits(limits);
        }
    }

    private final class IndexControlPlaneImpl implements IndexControlPlane {
        private final IndexRuntimeView runtime = new IndexRuntimeViewImpl();
        private final IndexConfigurationManagement configuration = new IndexConfigurationManagementImpl();

        @Override
        public String indexName() {
            return conf.getIndexName();
        }

        @Override
        public IndexRuntimeView runtime() {
            return runtime;
        }

        @Override
        public IndexConfigurationManagement configuration() {
            return configuration;
        }
    }

    private final class IndexRuntimeViewImpl implements IndexRuntimeView {

        @Override
        public IndexRuntimeSnapshot snapshot() {
            return new IndexRuntimeSnapshot(conf.getIndexName(), getState(),
                    metricsSnapshot(), Instant.now());
        }
    }

    private final class IndexConfigurationManagementImpl
            implements IndexConfigurationManagement {
        @Override
        public ConfigurationSnapshot getConfigurationActual() {
            return runtimeTuningState.snapshotCurrent();
        }

        @Override
        public ConfigurationSnapshot getConfigurationOriginal() {
            return runtimeTuningState.snapshotOriginal();
        }

        @Override
        public RuntimePatchValidation validate(final RuntimeConfigPatch patch) {
            return validateRuntimePatch(patch);
        }

        @Override
        public RuntimePatchResult apply(final RuntimeConfigPatch patch) {
            return applyRuntimePatch(patch);
        }
    }

    private static final class RuntimeTuningState {
        private final String indexName;
        private final EnumMap<RuntimeSettingKey, Integer> baseline;
        private final EnumMap<RuntimeSettingKey, Integer> overrides = new EnumMap<>(
                RuntimeSettingKey.class);
        private final AtomicLong revision = new AtomicLong(0L);

        private RuntimeTuningState(final String indexName,
                final EnumMap<RuntimeSettingKey, Integer> baseline) {
            this.indexName = Vldtn.requireNonNull(indexName, "indexName");
            this.baseline = Vldtn.requireNonNull(baseline, "baseline");
        }

        private static <K, V> RuntimeTuningState fromConfiguration(
                final IndexConfiguration<K, V> configuration) {
            final EnumMap<RuntimeSettingKey, Integer> baselineValues = new EnumMap<>(
                    RuntimeSettingKey.class);
            baselineValues.put(
                    RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                    configuration.getMaxNumberOfSegmentsInCache());
            baselineValues.put(
                    RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
                    configuration.getMaxNumberOfKeysInSegmentCache());
            baselineValues.put(
                    RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE,
                    configuration.getMaxNumberOfKeysInSegmentWriteCache());
            baselineValues.put(
                    RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE_DURING_MAINTENANCE,
                    configuration
                            .getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance());
            return new RuntimeTuningState(configuration.getIndexName(),
                    baselineValues);
        }

        private synchronized ConfigurationSnapshot snapshotCurrent() {
            return new ConfigurationSnapshot(indexName,
                    effectiveFromOverrides(overrides), revision.get(),
                    Instant.now());
        }

        private synchronized ConfigurationSnapshot snapshotOriginal() {
            return new ConfigurationSnapshot(indexName, baseline,
                    revision.get(), Instant.now());
        }

        private synchronized ConfigurationSnapshot apply(
                final Map<RuntimeSettingKey, Integer> patchValues) {
            overrides.putAll(patchValues);
            final long nextRevision = revision.incrementAndGet();
            return new ConfigurationSnapshot(indexName,
                    effectiveFromOverrides(overrides), nextRevision,
                    Instant.now());
        }

        private synchronized Map<RuntimeSettingKey, Integer> previewEffective(
                final Map<RuntimeSettingKey, Integer> patchValues) {
            final EnumMap<RuntimeSettingKey, Integer> mergedOverrides = new EnumMap<>(
                    RuntimeSettingKey.class);
            mergedOverrides.putAll(overrides);
            mergedOverrides.putAll(patchValues);
            return effectiveFromOverrides(mergedOverrides);
        }

        private synchronized int effectiveValue(final RuntimeSettingKey key) {
            final Integer override = overrides.get(key);
            if (override != null) {
                return override.intValue();
            }
            return baseline.get(key).intValue();
        }

        private synchronized long revision() {
            return revision.get();
        }

        private EnumMap<RuntimeSettingKey, Integer> effectiveFromOverrides(
                final Map<RuntimeSettingKey, Integer> overrideValues) {
            final EnumMap<RuntimeSettingKey, Integer> effective = new EnumMap<>(
                    RuntimeSettingKey.class);
            effective.putAll(baseline);
            effective.putAll(overrideValues);
            return effective;
        }
    }

    private static final class SegmentRuntimeAggregate {
        private int segmentCount;
        private int segmentReadyCount;
        private int segmentMaintenanceCount;
        private int segmentErrorCount;
        private int segmentClosedCount;
        private int segmentBusyCount;
        private long totalSegmentKeys;
        private long totalSegmentCacheKeys;
        private long totalWriteCacheKeys;
        private long totalDeltaCacheFiles;
        private long compactRequestCount;
        private long flushRequestCount;
        private long bloomFilterRequestCount;
        private long bloomFilterRefusedCount;
        private long bloomFilterPositiveCount;
        private long bloomFilterFalsePositiveCount;
        private final List<SegmentIndexMetricsSnapshot.SegmentMetricsSnapshot> segmentRuntimeSnapshots = new ArrayList<>();
    }

    /** {@inheritDoc} */
    @Override
    public IndexConfiguration<K, V> getConfiguration() {
        return conf;
    }

}
