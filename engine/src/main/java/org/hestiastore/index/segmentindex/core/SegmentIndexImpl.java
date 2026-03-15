package org.hestiastore.index.segmentindex.core;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorList;
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
import org.hestiastore.index.segment.SegmentDirectoryLayout;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
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
import org.hestiastore.index.segmentindex.partition.PartitionImmutableRun;
import org.hestiastore.index.segmentindex.partition.PartitionLookupResult;
import org.hestiastore.index.segmentindex.partition.PartitionRuntime;
import org.hestiastore.index.segmentindex.partition.PartitionRuntimeLimits;
import org.hestiastore.index.segmentindex.partition.PartitionRuntimeSnapshot;
import org.hestiastore.index.segmentindex.partition.PartitionWriteResultStatus;
import org.hestiastore.index.segmentindex.split.PartitionStableSplitCoordinator;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentFactory;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;
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

    private static final String OPERATION_COMPACT = "compact";
    private static final String OPERATION_FLUSH = "flush";
    private static final String OPERATION_DRAIN = "drain";
    private static final String OPERATION_OPEN_FULL_ISOLATION_ITERATOR = "openFullIsolationIterator";
    private static final String OPERATION_CLEANUP_ORPHAN_SEGMENT = "cleanupOrphanSegment";
    private static final String INDEX_NAME_MDC_KEY = "index.name";
    private static final int DEFAULT_MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION = 2;
    private static final long BACKGROUND_SPLIT_POLICY_INTERVAL_MILLIS = 250L;
    private static final long WAL_RETENTION_PRESSURE_WARN_INTERVAL_NANOS = TimeUnit.SECONDS
            .toNanos(5L);
    private static final Pattern SEGMENT_DIRECTORY_PATTERN = Pattern
            .compile("^segment-(\\d{5})$");
    private static final int BOOTSTRAP_SEGMENT_ID = 0;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Directory directoryFacade;
    private final IndexConfiguration<K, V> conf;
    protected final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentFactory<K, V> segmentFactory;
    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;
    private final SegmentIndexCore<K, V> core;
    private final PartitionRuntime<K, V> partitionRuntime;
    private final Executor drainExecutor;
    private final ScheduledExecutorService splitPolicyScheduler;
    private final IndexRetryPolicy retryPolicy;
    private final RuntimeTuningState runtimeTuningState;
    private final IndexControlPlane controlPlane;
    private final WalRuntime<K, V> walRuntime;
    private final Stats stats = new Stats();
    private final AtomicLong compactRequestHighWaterMark = new AtomicLong();
    private final AtomicLong flushRequestHighWaterMark = new AtomicLong();
    private final AtomicLong walRetentionPressureLastWarnNanos = new AtomicLong(
            0L);
    private final AtomicBoolean walRetentionPressureWarnActive = new AtomicBoolean(
            false);
    private final AtomicBoolean backgroundSplitScanScheduled = new AtomicBoolean(
            false);
    private final AtomicBoolean backgroundSplitScanRequested = new AtomicBoolean(
            false);
    private final AtomicBoolean backgroundSplitPolicyTickScheduled = new AtomicBoolean(
            false);
    private final ConcurrentHashMap<SegmentId, Boolean> backgroundSplitHintedSegments = new ConcurrentHashMap<>();
    private final Object asyncMonitor = new Object();
    private int asyncInFlight = 0;
    private final ThreadLocal<Boolean> inAsyncOperation = ThreadLocal
            .withInitial(() -> Boolean.FALSE);
    private final AtomicLong lastAppliedWalLsn = new AtomicLong(0L);
    private boolean startupConsistencyCheckForStaleSegmentLocks;
    private volatile IndexState<K, V> indexState;
    private volatile SegmentIndexState segmentIndexState = SegmentIndexState.OPENING;

    protected SegmentIndexImpl(final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final IndexExecutorRegistry executorRegistry) {
        final Directory nonNullDirectory = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.directoryFacade = nonNullDirectory;
        final IndexStateOpening<K, V> openingState = new IndexStateOpening<>(
                nonNullDirectory);
        setIndexState(openingState);
        setSegmentIndexState(SegmentIndexState.OPENING);
        try {
            this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                    "keyTypeDescriptor");
            this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                    "valueTypeDescriptor");
            this.conf = Vldtn.requireNonNull(conf, "conf");
            final String previousIndexName = MDC.get(INDEX_NAME_MDC_KEY);
            final boolean contextApplied = applyIndexContext(this.conf);
            try {
                logger.debug("Opening index '{}'.", conf.getIndexName());
                Vldtn.requireNonNull(executorRegistry, "executorRegistry");
                this.runtimeTuningState = RuntimeTuningState
                        .fromConfiguration(conf);
                final KeyToSegmentMap<K> keyToSegmentMapDelegate = new KeyToSegmentMap<>(
                        nonNullDirectory, keyTypeDescriptor);
                this.keyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                        keyToSegmentMapDelegate);
                this.segmentFactory = new SegmentFactory<>(nonNullDirectory,
                        keyTypeDescriptor, valueTypeDescriptor, conf,
                        executorRegistry.getStableSegmentMaintenanceExecutor());
                this.segmentRegistry = SegmentRegistry.<K, V>builder()
                        .withDirectoryFacade(nonNullDirectory)
                        .withKeyTypeDescriptor(keyTypeDescriptor)
                        .withValueTypeDescriptor(valueTypeDescriptor)
                        .withConfiguration(conf)
                        .withSegmentMaintenanceExecutor(
                                executorRegistry
                                        .getStableSegmentMaintenanceExecutor())
                        .withRegistryMaintenanceExecutor(
                                executorRegistry.getRegistryMaintenanceExecutor())
                        .build();
                final PartitionRuntime<K, V> partitionRuntime = new PartitionRuntime<>(
                        keyTypeDescriptor.getComparator());
                this.partitionRuntime = partitionRuntime;
                final PartitionStableSplitCoordinator<K, V> splitCoordinator = new PartitionStableSplitCoordinator<>(
                        conf, keyTypeDescriptor.getComparator(), keyToSegmentMap, segmentRegistry,
                        partitionRuntime);
                this.backgroundSplitCoordinator = new BackgroundSplitCoordinator<>(
                        keyToSegmentMap, partitionRuntime, splitCoordinator,
                        executorRegistry.getSplitMaintenanceExecutor(),
                        this::failWithError,
                        this::scheduleBackgroundSplitPolicyScanIfIdle);
                this.core = new SegmentIndexCore<>(keyToSegmentMap,
                        segmentRegistry);
                this.drainExecutor = executorRegistry
                        .getIndexMaintenanceExecutor();
                this.splitPolicyScheduler = executorRegistry
                        .getSplitPolicyScheduler();
                this.retryPolicy = new IndexRetryPolicy(
                        conf.getIndexBusyBackoffMillis(),
                        conf.getIndexBusyTimeoutMillis());
                this.controlPlane = new IndexControlPlaneImpl();
                this.walRuntime = WalRuntime.open(nonNullDirectory, conf.getWal(),
                        keyTypeDescriptor, valueTypeDescriptor);
                if (walRuntime.isEnabled()) {
                    final WalRuntime.RecoveryResult recoveryResult = walRuntime
                            .recover(this::replayWalRecord);
                    if (recoveryResult.maxLsn() > 0L) {
                        lastAppliedWalLsn.set(Math.max(lastAppliedWalLsn.get(),
                                recoveryResult.maxLsn()));
                    }
                }
                cleanupOrphanedSegmentDirectories();
                getIndexState().onReady(this);
                setSegmentIndexState(SegmentIndexState.READY);
                if (openingState.wasStaleLockRecovered()) {
                    logger.info(
                            "Recovered stale index lock (.lock). Index is going to be checked for consistency and unlocked.");
                    startupConsistencyCheckForStaleSegmentLocks = true;
                    try {
                        checkAndRepairConsistency();
                    } finally {
                        startupConsistencyCheckForStaleSegmentLocks = false;
                    }
                }
                scheduleBackgroundSplitPolicyScan();
                logger.debug("Index '{}' opened.", conf.getIndexName());
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
        final long walLsn = appendWalPutWithFailureHandling(key, value);

        final IndexResult<Void> result = retryWhileBusy(
                () -> putBuffered(key, value), "put", key, false);
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
        final EntryIterator<K, V> segmentIterator;
        if (isolation == SegmentIteratorIsolation.FULL_ISOLATION) {
            segmentIterator = backgroundSplitCoordinator.runWithStableWriteAdmission(
                    () -> openMergedIteratorWithRouteSnapshot(resolvedWindows,
                            isolation));
        } else {
            segmentIterator = openMergedIterator(resolvedWindows, isolation);
        }
        if (isContextLoggingEnabled()) {
            return new EntryIteratorLoggingContext<>(segmentIterator, conf);
        }
        return segmentIterator;
    }

    /** {@inheritDoc} */
    @Override
    public void compact() {
        getIndexState().tryPerformOperation();
        drainPartitions(false);
        final PartitionRuntimeSnapshot partitionSnapshot = partitionRuntime
                .snapshot();
        if (partitionSnapshot.getDrainInFlightCount() > 0
                || partitionSnapshot.getActivePartitionCount() > 0
                || partitionSnapshot.getImmutableRunCount() > 0
                || partitionSnapshot.getBufferedKeyCount() > 0) {
            return;
        }
        if (backgroundSplitCoordinator.splitInFlightCount() > 0) {
            return;
        }
        backgroundSplitCoordinator.runWithSplitSchedulingPaused(() -> keyToSegmentMap
                .getSegmentIds()
                .forEach(segmentId -> compactSegment(segmentId, false)));
        scheduleBackgroundSplitPolicyScanIfIdle();
    }

    /** {@inheritDoc} */
    @Override
    public void compactAndWait() {
        getIndexState().tryPerformOperation();
        drainPartitions(true);
        awaitBackgroundSplitPolicySettled();
        drainPartitions(true);
        awaitBackgroundSplitPolicySettled();
        backgroundSplitCoordinator.runWithSplitSchedulingPaused(() -> {
            keyToSegmentMap.getSegmentIds()
                    .forEach(segmentId -> compactSegment(segmentId, true));
            flushSegments(true);
        });
        scheduleBackgroundSplitPolicyScanIfIdle();
        awaitBackgroundSplitPolicySettled();
        keyToSegmentMap.optionalyFlush();
        checkpointWal();
    }

    /** {@inheritDoc} */
    @Override
    public V get(final K key) {
        final long startedNanos = System.nanoTime();
        getIndexState().tryPerformOperation();
        Vldtn.requireNonNull(key, "key");
        stats.incGetCx();

        final IndexResult<V> result = retryWhileBusy(
                () -> backgroundSplitCoordinator
                        .runWithStableWriteAdmission(() -> getBuffered(key)),
                "get", key, true);
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
        final IndexResult<Void> result = retryWhileBusy(
                () -> putBuffered(key, valueTypeDescriptor.getTombstone()),
                "delete", key, false);
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
                keyToSegmentMap, segmentRegistry, keyTypeDescriptor,
                startupConsistencyCheckForStaleSegmentLocks
                        ? this::hasSegmentLockFile
                        : segmentId -> true);
        checker.checkAndRepairConsistency();
        cleanupOrphanedSegmentDirectories();
        scheduleBackgroundSplitPolicyScan();
    }

    private boolean hasSegmentLockFile(final SegmentId segmentId) {
        final SegmentId nonNullSegmentId = Vldtn.requireNonNull(segmentId,
                "segmentId");
        final String segmentDirectoryName = nonNullSegmentId.getName();
        if (!directoryFacade.isFileExists(segmentDirectoryName)) {
            return false;
        }
        final Directory segmentDirectory = directoryFacade
                .openSubDirectory(segmentDirectoryName);
        final String lockFileName = new SegmentDirectoryLayout(nonNullSegmentId)
                .getLockFileName();
        return segmentDirectory.isFileExists(lockFileName);
    }

    /** {@inheritDoc} */
    @Override
    protected void doClose() {
        logger.debug("Closing index '{}'.", conf.getIndexName());
        try {
            getIndexState().onClose(this);
            setSegmentIndexState(SegmentIndexState.CLOSING);
            awaitAsyncOperations();
            drainPartitions(true);
            awaitBackgroundSplitPolicySettled();
            drainPartitions(true);
            awaitBackgroundSplitPolicySettled();
            setSegmentIndexState(SegmentIndexState.CLOSED);
            backgroundSplitCoordinator
                    .runWithSplitSchedulingPaused(() -> flushSegments(true));
            final SegmentRegistryResult<Void> closeResult = segmentRegistry
                    .close();
            if (closeResult.getStatus() != SegmentRegistryResultStatus.OK
                    && closeResult
                            .getStatus() != SegmentRegistryResultStatus.CLOSED) {
                throw new IndexException(
                        String.format("Index operation '%s' failed: %s",
                                "close", closeResult.getStatus()));
            }
            keyToSegmentMap.optionalyFlush();
            checkpointWal();
            if (logger.isDebugEnabled()) {
                logger.debug(String.format(
                        "Index is closing, where was %s gets, %s puts and %s deletes.",
                        F.fmt(stats.getGetCx()), F.fmt(stats.getPutCx()),
                        F.fmt(stats.getDeleteCx())));
            }
            logger.debug("Index '{}' closed.", conf.getIndexName());
        } finally {
            try {
                completeCloseStateTransition();
            } finally {
                walRuntime.close();
            }
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
        final StableSegmentRuntimeAggregate stableSegmentRuntime =
                collectStableSegmentRuntime();
        final PartitionRuntimeSnapshot partitionSnapshot = partitionRuntime
                .snapshot();
        final var walStats = walRuntime.statsSnapshot();
        final long compactRequestCount = Math.max(stats.getCompactRequestCx(),
                updateHighWaterMark(compactRequestHighWaterMark,
                        stableSegmentRuntime.totalCompactRequestCount));
        final long flushRequestCount = Math.max(stats.getFlushRequestCx(),
                updateHighWaterMark(flushRequestHighWaterMark,
                        stableSegmentRuntime.totalFlushRequestCount));
        // Keep legacy segment-centric metric field names for snapshot/API
        // compatibility while the implementation is partition-first.
        return new SegmentIndexMetricsSnapshot(stats.getGetCx(),
                stats.getPutCx(), stats.getDeleteCx(), cacheStats.hitCount(),
                cacheStats.missCount(), cacheStats.loadCount(),
                cacheStats.evictionCount(), cacheStats.size(),
                cacheStats.limit(),
                runtimeTuningState.effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE),
                runtimeTuningState.effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION),
                runtimeTuningState.effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER),
                stableSegmentRuntime.totalMappedStableSegmentCount,
                stableSegmentRuntime.readyStableSegmentCount,
                stableSegmentRuntime.stableSegmentsInMaintenanceStateCount,
                stableSegmentRuntime.errorStableSegmentCount,
                stableSegmentRuntime.closedStableSegmentCount,
                stableSegmentRuntime.unloadedMappedStableSegmentCount,
                stableSegmentRuntime.totalStableSegmentKeyCount,
                stableSegmentRuntime.totalStableSegmentCacheKeyCount,
                stableSegmentRuntime.totalStableSegmentWriteBufferKeyCount
                        + partitionSnapshot.getBufferedKeyCount(),
                stableSegmentRuntime.totalStableSegmentDeltaCacheFileCount,
                compactRequestCount, flushRequestCount,
                partitionSnapshot.getDrainScheduleCount(),
                partitionSnapshot.getDrainInFlightCount(),
                partitionSnapshot.getImmutableRunCount(),
                runtimeTuningState
                        .effectiveValue(
                                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER),
                partitionSnapshot.getDrainingPartitionCount(),
                runtimeTuningState.effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION),
                stats.getReadLatencyP50Micros(),
                stats.getReadLatencyP95Micros(),
                stats.getReadLatencyP99Micros(),
                stats.getWriteLatencyP50Micros(),
                stats.getWriteLatencyP95Micros(),
                stats.getWriteLatencyP99Micros(),
                conf.getBloomFilterNumberOfHashFunctions(),
                conf.getBloomFilterIndexSizeInBytes(),
                conf.getBloomFilterProbabilityOfFalsePositive(),
                stableSegmentRuntime.totalBloomFilterRequestCount,
                stableSegmentRuntime.totalBloomFilterRefusedCount,
                stableSegmentRuntime.totalBloomFilterPositiveCount,
                stableSegmentRuntime.totalBloomFilterFalsePositiveCount,
                walRuntime.isEnabled(), walStats.appendCount(),
                walStats.appendBytes(), walStats.syncCount(),
                walStats.syncFailureCount(), walStats.corruptionCount(),
                walStats.truncationCount(), walStats.retainedBytes(),
                walStats.segmentCount(), walStats.durableLsn(),
                walStats.checkpointLsn(), walStats.pendingSyncBytes(),
                lastAppliedWalLsn.get(), walStats.syncTotalNanos(),
                walStats.syncMaxNanos(), walStats.syncBatchBytesTotal(),
                walStats.syncBatchBytesMax(),
                runtimeTuningState.effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION),
                runtimeTuningState.effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER),
                partitionSnapshot.getPartitionCount(),
                partitionSnapshot.getActivePartitionCount(),
                partitionSnapshot.getDrainingPartitionCount(),
                partitionSnapshot.getImmutableRunCount(),
                partitionSnapshot.getBufferedKeyCount(),
                partitionSnapshot.getLocalThrottleCount(),
                partitionSnapshot.getGlobalThrottleCount(),
                partitionSnapshot.getDrainScheduleCount(),
                partitionSnapshot.getDrainInFlightCount(),
                stats.getDrainLatencyP95Micros(),
                stableSegmentRuntime.stableSegmentMetricsSnapshots,
                getState());
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
        } else if (currentState instanceof IndexStateClosing<?, ?> closingState) {
            fileLock = closingState.getFileLock();
        }
        setIndexState(new IndexStateError<>(failure, fileLock));
    }

    /** {@inheritDoc} */
    @Override
    public void flush() {
        drainPartitions(false);
        backgroundSplitCoordinator
                .runWithSplitSchedulingPaused(() -> flushSegments(false));
        keyToSegmentMap.optionalyFlush();
        scheduleBackgroundSplitPolicyScanIfIdle();
    }

    /** {@inheritDoc} */
    @Override
    public void flushAndWait() {
        drainPartitions(true);
        awaitBackgroundSplitPolicySettled();
        drainPartitions(true);
        awaitBackgroundSplitPolicySettled();
        flushMappedSegmentsAndWait();
        final long finalTopologyVersion = keyToSegmentMap.snapshot().version();
        scheduleBackgroundSplitPolicyScanIfIdle();
        awaitBackgroundSplitPolicySettled();
        if (!keyToSegmentMap.isVersion(finalTopologyVersion)) {
            drainPartitions(true);
            awaitBackgroundSplitPolicySettled();
            flushMappedSegmentsAndWait();
        }
        keyToSegmentMap.optionalyFlush();
        checkpointWal();
    }

    private IndexResult<Void> putBuffered(final K key, final V value) {
        return backgroundSplitCoordinator.runWithStableWriteAdmission(() -> {
            final IndexResult<SegmentId> routeResult = resolveWriteSegmentId(
                    key);
            if (routeResult.getStatus() != IndexResultStatus.OK
                    || routeResult.getValue() == null) {
                return toVoidResult(routeResult.getStatus());
            }
            final SegmentId segmentId = routeResult.getValue();
            partitionRuntime.ensurePartition(segmentId);
            final var writeResult = partitionRuntime.write(segmentId, key,
                    value, currentPartitionRuntimeLimits());
            if (writeResult.getStatus() == PartitionWriteResultStatus.BUSY) {
                return IndexResult.busy();
            }
            if (writeResult.isDrainRecommended()) {
                schedulePartitionDrain(segmentId);
            }
            return IndexResult.ok();
        });
    }

    private IndexResult<V> getBuffered(final K key) {
        final KeyToSegmentMap.Snapshot<K> snapshot = keyToSegmentMap.snapshot();
        final SegmentId segmentId = snapshot.findSegmentId(key);
        if (segmentId == null) {
            return IndexResult.ok(null);
        }
        partitionRuntime.ensurePartition(segmentId);
        final PartitionLookupResult<V> overlay = partitionRuntime.lookup(
                segmentId, key);
        if (overlay.isFound()) {
            final V value = overlay.getValue();
            return IndexResult.ok(
                    valueTypeDescriptor.isTombstone(value) ? null : value);
        }
        if (!keyToSegmentMap.isMappingValid(key, segmentId,
                snapshot.version())) {
            return IndexResult.busy();
        }
        return core.get(segmentId, key);
    }

    private IndexResult<SegmentId> resolveWriteSegmentId(final K key) {
        final KeyToSegmentMap.Snapshot<K> snapshot = keyToSegmentMap.snapshot();
        if (snapshot.findSegmentId(key) == null
                && !keyToSegmentMap.tryExtendMaxKey(key, snapshot)) {
            return IndexResult.busy();
        }
        final KeyToSegmentMap.Snapshot<K> stableSnapshot = keyToSegmentMap
                .snapshot();
        final SegmentId segmentId = stableSnapshot.findSegmentId(key);
        if (segmentId == null) {
            return IndexResult.busy();
        }
        return IndexResult.ok(segmentId);
    }

    private PartitionRuntimeLimits currentPartitionRuntimeLimits() {
        final int maxActive = Math.max(1, runtimeTuningState.effectiveValue(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION));
        final int maxImmutableRuns = Math.max(1, runtimeTuningState
                .effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION));
        final int maxPartitionBuffer = Math.max(maxActive + 1,
                runtimeTuningState.effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER));
        final int maxIndexBuffer = Math.max(maxPartitionBuffer, runtimeTuningState
                .effectiveValue(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER));
        return new PartitionRuntimeLimits(maxActive, maxImmutableRuns,
                maxPartitionBuffer, maxIndexBuffer);
    }

    private void schedulePartitionDrain(final SegmentId segmentId) {
        if (!partitionRuntime.markDrainScheduledIfNeeded(segmentId)) {
            return;
        }
        try {
            drainExecutor.execute(() -> drainPartitionLoop(segmentId));
        } catch (final RuntimeException e) {
            partitionRuntime.finishDrainScheduling(segmentId);
            throw e;
        }
    }

    private void drainPartitions(final boolean waitForCompletion) {
        partitionRuntime.sealAllActivePartitionsForDrain();
        final List<SegmentId> segmentIds = keyToSegmentMap.getSegmentIds();
        for (final SegmentId segmentId : segmentIds) {
            if (waitForCompletion) {
                drainPartitionNow(segmentId);
            } else {
                schedulePartitionDrain(segmentId);
            }
        }
        if (!waitForCompletion) {
            return;
        }
        final long startNanos = retryPolicy.startNanos();
        while (partitionRuntime.snapshot().getDrainInFlightCount() > 0) {
            retryPolicy.backoffOrThrow(startNanos, OPERATION_DRAIN, null);
        }
    }

    private void drainPartitionNow(final SegmentId segmentId) {
        if (!partitionRuntime.markDrainScheduledIfNeeded(segmentId)) {
            return;
        }
        drainPartitionLoop(segmentId);
    }

    private void drainPartitionLoop(final SegmentId segmentId) {
        boolean drainedAnyRun = false;
        try {
            while (true) {
                final PartitionImmutableRun<K, V> run = partitionRuntime
                        .peekOldestImmutableRun(segmentId);
                if (run == null) {
                    return;
                }
                final long drainRunStartNanos = System.nanoTime();
                drainImmutableRun(segmentId, run);
                partitionRuntime.completeDrainedRun(segmentId, run);
                stats.recordDrainLatencyNanos(
                        System.nanoTime() - drainRunStartNanos);
                drainedAnyRun = true;
            }
        } catch (final RuntimeException e) {
            failWithError(e);
            throw e;
        } finally {
            partitionRuntime.finishDrainScheduling(segmentId);
            if (drainedAnyRun && isBackgroundSplitPolicyEnabled()) {
                scheduleBackgroundSplitPolicyHint(segmentId);
            }
        }
    }

    private void drainImmutableRun(final SegmentId segmentId,
            final PartitionImmutableRun<K, V> run) {
        for (final Map.Entry<K, V> entry : run.getEntries().entrySet()) {
            putStableEntry(segmentId, entry.getKey(), entry.getValue());
        }
        flushSegment(segmentId, true);
    }

    private void putStableEntry(final SegmentId segmentId, final K key,
            final V value) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentRegistryResult<Segment<K, V>> loaded = segmentRegistry
                    .getSegment(segmentId);
            if (loaded.getStatus() == SegmentRegistryResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, OPERATION_DRAIN,
                        segmentId);
                continue;
            }
            if (loaded.getStatus() != SegmentRegistryResultStatus.OK
                    || loaded.getValue() == null) {
                throw new IndexException(String.format(
                        "Segment '%s' failed to load for drain: %s", segmentId,
                        loaded.getStatus()));
            }
            final SegmentResult<Void> putResult = loaded.getValue().put(key,
                    value);
            if (putResult.getStatus() == SegmentResultStatus.OK) {
                return;
            }
            if (putResult.getStatus() == SegmentResultStatus.BUSY
                    || putResult.getStatus() == SegmentResultStatus.CLOSED) {
                retryPolicy.backoffOrThrow(startNanos, OPERATION_DRAIN,
                        segmentId);
                continue;
            }
            throw new IndexException(String.format(
                    "Segment '%s' failed to accept drain entry: %s", segmentId,
                    putResult.getStatus()));
        }
    }

    private void scheduleBackgroundSplitPolicyScan() {
        if (!isBackgroundSplitPolicyEnabled()) {
            return;
        }
        ensureAutonomousBackgroundSplitPolicyLoop();
        backgroundSplitScanRequested.set(true);
        scheduleBackgroundSplitPolicyWorker();
    }

    private void scheduleBackgroundSplitPolicyScanIfIdle() {
        if (!isBackgroundSplitPolicyEnabled()) {
            return;
        }
        ensureAutonomousBackgroundSplitPolicyLoop();
        if (!isAutonomousSplitPolicyIdle()) {
            return;
        }
        scheduleBackgroundSplitPolicyScan();
    }

    private void scheduleBackgroundSplitPolicyHint(final SegmentId segmentId) {
        if (!isBackgroundSplitPolicyEnabled()) {
            return;
        }
        if (segmentId == null || !isSegmentStillMapped(segmentId)) {
            return;
        }
        backgroundSplitHintedSegments.put(segmentId, Boolean.TRUE);
        ensureAutonomousBackgroundSplitPolicyLoop();
        scheduleBackgroundSplitPolicyWorker();
    }

    private void scheduleBackgroundSplitPolicyWorker() {
        if (!backgroundSplitScanScheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            drainExecutor.execute(this::runBackgroundSplitPolicyLoop);
        } catch (final RuntimeException e) {
            backgroundSplitScanScheduled.set(false);
            if (isClosedOrClosingState()) {
                return;
            }
            throw e;
        }
    }

    private void runBackgroundSplitPolicyLoop() {
        try {
            if (!isBackgroundSplitPolicyEnabled()) {
                return;
            }
            do {
                final boolean fullScanRequested = backgroundSplitScanRequested
                        .getAndSet(false);
                scanHintedSplitCandidates();
                if (fullScanRequested) {
                    scanCurrentSplitCandidates();
                }
            } while (backgroundSplitScanRequested.get()
                    || hasPendingSplitHints());
        } catch (final RuntimeException e) {
            if (!isClosedOrClosingState()) {
                failWithError(e);
                throw e;
            }
        } finally {
            backgroundSplitScanScheduled.set(false);
            if ((backgroundSplitScanRequested.get()
                    || hasPendingSplitHints())
                    && isBackgroundSplitPolicyEnabled()) {
                scheduleBackgroundSplitPolicyWorker();
            }
        }
    }

    private void ensureAutonomousBackgroundSplitPolicyLoop() {
        if (!isBackgroundSplitPolicyEnabled()) {
            return;
        }
        if (!backgroundSplitPolicyTickScheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            splitPolicyScheduler.schedule(
                    this::runAutonomousBackgroundSplitPolicyTick,
                    BACKGROUND_SPLIT_POLICY_INTERVAL_MILLIS,
                    TimeUnit.MILLISECONDS);
        } catch (final RuntimeException e) {
            backgroundSplitPolicyTickScheduled.set(false);
            if (isClosedOrClosingState()) {
                return;
            }
            throw e;
        }
    }

    private void runAutonomousBackgroundSplitPolicyTick() {
        backgroundSplitPolicyTickScheduled.set(false);
        try {
            if (!isBackgroundSplitPolicyEnabled()) {
                return;
            }
            if (isAutonomousSplitPolicyIdle()) {
                scheduleBackgroundSplitPolicyScan();
            }
        } catch (final RuntimeException e) {
            if (!isClosedOrClosingState()) {
                failWithError(e);
                throw e;
            }
        } finally {
            if (isBackgroundSplitPolicyEnabled()) {
                ensureAutonomousBackgroundSplitPolicyLoop();
            }
        }
    }

    private boolean isAutonomousSplitPolicyIdle() {
        final PartitionRuntimeSnapshot snapshot = partitionRuntime.snapshot();
        return snapshot.getBufferedKeyCount() == 0
                && snapshot.getActivePartitionCount() == 0
                && snapshot.getImmutableRunCount() == 0
                && snapshot.getDrainInFlightCount() == 0
                && snapshot.getDrainingPartitionCount() == 0
                && backgroundSplitCoordinator.splitInFlightCount() == 0;
    }

    private void scanCurrentSplitCandidates() {
        final int threshold = runtimeTuningState.effectiveValue(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT);
        if (threshold < 1) {
            return;
        }
        for (final SegmentId segmentId : keyToSegmentMap.getSegmentIds()) {
            if (!isSegmentStillMapped(segmentId)) {
                continue;
            }
            final Segment<K, V> segment = tryLoadSplitCandidate(segmentId);
            if (segment == null) {
                continue;
            }
            if (backgroundSplitCoordinator.handleSplitCandidate(segment,
                    (long) threshold)) {
                stats.incSplitScheduleCx();
            }
        }
    }

    private void scanHintedSplitCandidates() {
        final int threshold = runtimeTuningState.effectiveValue(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT);
        if (threshold < 1) {
            backgroundSplitHintedSegments.clear();
            return;
        }
        final List<SegmentId> hintedSegmentIds = new ArrayList<>(
                backgroundSplitHintedSegments.keySet());
        for (final SegmentId segmentId : hintedSegmentIds) {
            backgroundSplitHintedSegments.remove(segmentId);
            if (!isSegmentStillMapped(segmentId)) {
                continue;
            }
            final Segment<K, V> segment = tryLoadSplitCandidate(segmentId);
            if (segment == null) {
                continue;
            }
            if (backgroundSplitCoordinator.handleSplitCandidate(segment,
                    (long) threshold)) {
                stats.incSplitScheduleCx();
            }
        }
    }

    private boolean hasPendingSplitHints() {
        return !backgroundSplitHintedSegments.isEmpty();
    }

    private Segment<K, V> tryLoadSplitCandidate(final SegmentId segmentId) {
        final SegmentRegistryResult<Segment<K, V>> loaded = segmentRegistry
                .getSegment(segmentId);
        if (loaded.getStatus() == SegmentRegistryResultStatus.OK) {
            return loaded.getValue();
        }
        if (loaded.getStatus() == SegmentRegistryResultStatus.BUSY
                || loaded.getStatus() == SegmentRegistryResultStatus.CLOSED) {
            return null;
        }
        if (!isSegmentStillMapped(segmentId)) {
            return null;
        }
        throw new IndexException(String.format(
                "Segment '%s' failed to load for split scheduling: %s",
                segmentId, loaded.getStatus()));
    }

    private boolean isBackgroundSplitPolicyEnabled() {
        return Boolean.TRUE.equals(conf.isBackgroundMaintenanceAutoEnabled())
                && !isClosedOrClosingState();
    }

    private boolean isClosedOrClosingState() {
        final SegmentIndexState state = getState();
        return state == SegmentIndexState.CLOSED
                || state == SegmentIndexState.ERROR;
    }

    private void completeCloseStateTransition() {
        if (getState() != SegmentIndexState.ERROR) {
            setSegmentIndexState(SegmentIndexState.CLOSED);
        }
        final IndexState<K, V> currentState = indexState;
        if (currentState instanceof IndexStateClosing<?, ?>) {
            @SuppressWarnings("unchecked")
            final IndexStateClosing<K, V> closingState = (IndexStateClosing<K, V>) currentState;
            closingState.finishClose(this);
            return;
        }
        if (currentState instanceof IndexStateClosed<?, ?>) {
            return;
        }
        currentState.onClose(this);
    }

    private void cleanupOrphanedSegmentDirectories() {
        final Set<SegmentId> mappedSegmentIds = new HashSet<>(
                keyToSegmentMap.getSegmentIds());
        final List<SegmentId> orphanedSegmentIds = new ArrayList<>();
        try (var fileNames = directoryFacade.getFileNames()) {
            fileNames.forEach(name -> {
                final SegmentId segmentId = parseSegmentDirectoryName(name);
                if (segmentId != null
                        && segmentId.getId() != BOOTSTRAP_SEGMENT_ID
                        && !mappedSegmentIds.contains(segmentId)) {
                    orphanedSegmentIds.add(segmentId);
                }
            });
        }
        orphanedSegmentIds.forEach(this::deleteOrphanedSegmentDirectory);
    }

    private SegmentId parseSegmentDirectoryName(final String name) {
        if (name == null) {
            return null;
        }
        final var matcher = SEGMENT_DIRECTORY_PATTERN.matcher(name);
        if (!matcher.matches()) {
            return null;
        }
        try {
            return SegmentId.of(Integer.parseInt(matcher.group(1)));
        } catch (final NumberFormatException e) {
            return null;
        }
    }

    private void deleteOrphanedSegmentDirectory(final SegmentId segmentId) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentRegistryResult<Void> deleteResult = segmentRegistry
                    .deleteSegment(segmentId);
            if (deleteResult.getStatus() == SegmentRegistryResultStatus.OK
                    || deleteResult
                            .getStatus() == SegmentRegistryResultStatus.CLOSED) {
                logger.info(
                        "Deleted orphaned segment directory '{}' during recovery/consistency cleanup.",
                        segmentId);
                return;
            }
            if (deleteResult.getStatus() == SegmentRegistryResultStatus.BUSY) {
                try {
                    retryPolicy.backoffOrThrow(startNanos,
                            OPERATION_CLEANUP_ORPHAN_SEGMENT, segmentId);
                } catch (final IndexException timeout) {
                    logger.warn(
                            "Orphaned segment directory '{}' could not be deleted because cleanup timed out.",
                            segmentId);
                    return;
                }
                continue;
            }
            logger.warn(
                    "Orphaned segment directory '{}' could not be deleted during cleanup: {}",
                    segmentId, deleteResult.getStatus());
            return;
        }
    }

    private EntryIterator<K, V> openMergedIterator(
            final SegmentWindow resolvedWindows,
            final SegmentIteratorIsolation isolation) {
        final List<SegmentId> segmentIds = keyToSegmentMap
                .getSegmentIds(resolvedWindows);
        return openMergedIterator(segmentIds, isolation);
    }

    private EntryIterator<K, V> openMergedIteratorWithRouteSnapshot(
            final SegmentWindow resolvedWindows,
            final SegmentIteratorIsolation isolation) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final KeyToSegmentMap.Snapshot<K> snapshot = keyToSegmentMap
                    .snapshot();
            final List<SegmentId> segmentIds = snapshot
                    .getSegmentIds(resolvedWindows);
            try {
                final EntryIterator<K, V> iterator = openMergedIterator(
                        segmentIds, isolation);
                if (keyToSegmentMap.isVersion(snapshot.version())) {
                    return iterator;
                }
                iterator.close();
            } catch (final IndexException e) {
                if (keyToSegmentMap.isVersion(snapshot.version())) {
                    throw e;
                }
            }
            retryPolicy.backoffOrThrow(startNanos,
                    OPERATION_OPEN_FULL_ISOLATION_ITERATOR, null);
        }
    }

    private EntryIterator<K, V> openMergedIterator(
            final List<SegmentId> segmentIds,
            final SegmentIteratorIsolation isolation) {
        final NavigableMap<K, V> merged = new TreeMap<>(
                keyTypeDescriptor.getComparator());
        try (EntryIterator<K, V> stableIterator = new SegmentsIterator<>(
                segmentIds, segmentRegistry, isolation, retryPolicy)) {
            while (stableIterator.hasNext()) {
                final Entry<K, V> entry = stableIterator.next();
                merged.put(entry.getKey(), entry.getValue());
            }
        }
        partitionRuntime.applyOverlaySnapshot(segmentIds, merged);
        final List<Entry<K, V>> visibleEntries = merged.entrySet().stream()
                .filter(entry -> !valueTypeDescriptor.isTombstone(
                        entry.getValue()))
                .map(entry -> Entry.of(entry.getKey(), entry.getValue()))
                .toList();
        return new EntryIteratorList<>(visibleEntries);
    }

    private void replayWalRecord(final WalRuntime.ReplayRecord<K, V> record) {
        final V value = record.getOperation() == WalRuntime.Operation.PUT
                ? record.getValue()
                : valueTypeDescriptor.getTombstone();
        final IndexResult<Void> result = retryWhileBusy(
                () -> putBuffered(record.getKey(), value), "walReplay",
                record.getKey(), false);
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
        logWalRetentionPressureStartIfNeeded();
        final long startNanos = retryPolicy.startNanos();
        int checkpointAttempts = 0;
        while (walRuntime.isRetentionPressure()) {
            checkpointAttempts++;
            drainPartitions(true);
            flushSegments(true);
            keyToSegmentMap.optionalyFlush();
            checkpointWal();
            if (!walRuntime.isRetentionPressure()) {
                logWalRetentionPressureCleared(startNanos, checkpointAttempts);
                return;
            }
            retryPolicy.backoffOrThrow(startNanos, "walBackpressure", null);
        }
        logWalRetentionPressureCleared(startNanos, checkpointAttempts);
    }

    private void logWalRetentionPressureStartIfNeeded() {
        final long nowNanos = System.nanoTime();
        while (true) {
            final long previousWarnNanos = walRetentionPressureLastWarnNanos
                    .get();
            if (previousWarnNanos != 0L && nowNanos
                    - previousWarnNanos < WAL_RETENTION_PRESSURE_WARN_INTERVAL_NANOS) {
                return;
            }
            if (walRetentionPressureLastWarnNanos
                    .compareAndSet(previousWarnNanos, nowNanos)) {
                walRetentionPressureWarnActive.set(true);
                logger.warn(
                        "event=wal_retention_pressure_start retainedBytes={} threshold={} action=force_checkpoint_backpressure",
                        walRuntime.retainedBytes(),
                        conf.getWal().getMaxBytesBeforeForcedCheckpoint());
                return;
            }
        }
    }

    private void logWalRetentionPressureCleared(final long startNanos,
            final int checkpointAttempts) {
        if (!walRetentionPressureWarnActive.compareAndSet(true, false)) {
            return;
        }
        final long elapsedMillis = TimeUnit.NANOSECONDS
                .toMillis(Math.max(0L, System.nanoTime() - startNanos));
        logger.info(
                "event=wal_retention_pressure_cleared retainedBytes={} threshold={} checkpointAttempts={} elapsedMillis={}",
                walRuntime.retainedBytes(),
                conf.getWal().getMaxBytesBeforeForcedCheckpoint(),
                Math.max(0, checkpointAttempts), Math.max(0L, elapsedMillis));
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
                "event=wal_sync_failure_transition state={} action=transition_to_error reason=wal_sync_failure",
                state, failure);
        failWithError(failure);
    }

    private void flushSegments(final boolean waitForCompletion) {
        keyToSegmentMap.getSegmentIds().forEach(
                segmentId -> flushSegment(segmentId, waitForCompletion));
    }

    private void flushMappedSegmentsAndWait() {
        backgroundSplitCoordinator
                .runWithSplitSchedulingPaused(() -> flushSegments(true));
    }

    private void compactMappedSegmentsAndFlush() {
        backgroundSplitCoordinator.runWithSplitSchedulingPaused(() -> {
            keyToSegmentMap.getSegmentIds()
                    .forEach(segmentId -> compactSegment(segmentId, true));
            flushSegments(true);
        });
    }
    private void compactSegment(final SegmentId segmentId,
            final boolean waitForCompletion) {
        if (logger.isDebugEnabled()) {
            logger.debug("Compact attempt started: segment='{}' wait='{}'",
                    segmentId, waitForCompletion);
        }
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final IndexResult<Segment<K, V>> result = core.compact(segmentId);
            final IndexResultStatus status = result.getStatus();
            if (status == IndexResultStatus.OK) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Compact accepted: segment='{}' wait='{}' state='{}'",
                            segmentId, waitForCompletion,
                            result.getValue() == null ? null
                                    : result.getValue().getState());
                }
                if (waitForCompletion) {
                    final Segment<K, V> segment = result.getValue();
                    if (segment != null) {
                        awaitSegmentReady(segmentId, OPERATION_COMPACT,
                                segment);
                    }
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Compact completed: segment='{}' wait='{}'",
                            segmentId, waitForCompletion);
                }
                return;
            }
            if (status == IndexResultStatus.CLOSED) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Compact skipped because segment is closed: segment='{}'",
                            segmentId);
                }
                return;
            }
            if (status == IndexResultStatus.BUSY) {
                if (!isSegmentStillMapped(segmentId)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "Compact aborted because segment is no longer mapped: segment='{}'",
                                segmentId);
                    }
                    return;
                }
                if (!waitForCompletion) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "Compact coalesced because segment is already busy: segment='{}'",
                                segmentId);
                    }
                    return;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Compact busy, retrying: segment='{}'",
                            segmentId);
                }
                retryPolicy.backoffOrThrow(startNanos, OPERATION_COMPACT,
                        segmentId);
                continue;
            }
            if (status == IndexResultStatus.ERROR
                    && !isSegmentStillMapped(segmentId)) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Compact ignored error because segment is no longer mapped: segment='{}'",
                            segmentId);
                }
                return;
            }
            throw newIndexException(OPERATION_COMPACT, segmentId, status);
        }
    }

    private void flushSegment(final SegmentId segmentId,
            final boolean waitForCompletion) {
        stats.incFlushRequestCx();
        if (logger.isDebugEnabled()) {
            logger.debug("Flush attempt started: segment='{}' wait='{}'",
                    segmentId, waitForCompletion);
        }
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final IndexResult<Segment<K, V>> result = core.flush(segmentId);
            final IndexResultStatus status = result.getStatus();
            if (status == IndexResultStatus.OK) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Flush accepted: segment='{}' wait='{}' state='{}'",
                            segmentId, waitForCompletion,
                            result.getValue() == null ? null
                                    : result.getValue().getState());
                }
                if (waitForCompletion) {
                    final Segment<K, V> segment = result.getValue();
                    if (segment != null) {
                        awaitSegmentReady(segmentId, OPERATION_FLUSH, segment);
                    }
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Flush completed: segment='{}' wait='{}'",
                            segmentId, waitForCompletion);
                }
                return;
            }
            if (status == IndexResultStatus.CLOSED) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Flush skipped because segment is closed: segment='{}'",
                            segmentId);
                }
                return;
            }
            if (status == IndexResultStatus.BUSY) {
                if (!isSegmentStillMapped(segmentId)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "Flush aborted because segment is no longer mapped: segment='{}'",
                                segmentId);
                    }
                    return;
                }
                if (!waitForCompletion) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "Flush coalesced because segment is already busy: segment='{}'",
                                segmentId);
                    }
                    return;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Flush busy, retrying: segment='{}'",
                            segmentId);
                }
                retryPolicy.backoffOrThrow(startNanos, OPERATION_FLUSH,
                        segmentId);
                continue;
            }
            if (status == IndexResultStatus.ERROR
                    && !isSegmentStillMapped(segmentId)) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Flush ignored error because segment is no longer mapped: segment='{}'",
                            segmentId);
                }
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

    private <T> IndexResult<T> retryWhileBusy(
            final Supplier<IndexResult<T>> operation, final String opName,
            final K key, final boolean retryClosed) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final IndexResult<T> result = operation.get();
            final IndexResultStatus status = result.getStatus();
            if (status == IndexResultStatus.BUSY
                    || (retryClosed && status == IndexResultStatus.CLOSED)) {
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

    private static IndexResult<Void> toVoidResult(
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
        backgroundSplitCoordinator
                .awaitSplitsIdle(conf.getIndexBusyTimeoutMillis());
    }

    private void awaitBackgroundSplitPolicySettled() {
        final long timeoutMillis = conf.getIndexBusyTimeoutMillis();
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            awaitSplitsIdle();
            if (!backgroundSplitScanRequested.get()
                    && !backgroundSplitScanScheduled.get()
                    && !hasPendingSplitHints()
                    && backgroundSplitCoordinator.splitInFlightCount() == 0) {
                return;
            }
            if (System.nanoTime() >= deadline) {
                throw new IndexException(String.format(
                        "Background split policy completion timed out after %d ms.",
                        timeoutMillis));
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
            if (Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                throw new IndexException(
                        "Interrupted while waiting for background split policy completion.");
            }
        }
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

    private StableSegmentRuntimeAggregate collectStableSegmentRuntime() {
        final StableSegmentRuntimeAggregate aggregate =
                new StableSegmentRuntimeAggregate();
        final List<SegmentId> mappedSegmentIds = keyToSegmentMap
                .getSegmentIds();
        aggregate.totalMappedStableSegmentCount = mappedSegmentIds.size();
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
                        aggregate.readyStableSegmentCount++;
                    } else if (state == SegmentState.MAINTENANCE_RUNNING
                            || state == SegmentState.FREEZE) {
                        aggregate.stableSegmentsInMaintenanceStateCount++;
                    } else if (state == SegmentState.ERROR) {
                        aggregate.errorStableSegmentCount++;
                    } else if (state == SegmentState.CLOSED) {
                        aggregate.closedStableSegmentCount++;
                    }
                    aggregate.totalStableSegmentKeyCount += Math.max(0L,
                            segmentRuntime.getNumberOfKeys());
                    aggregate.totalStableSegmentCacheKeyCount += Math.max(0L,
                            segmentRuntime.getNumberOfKeysInSegmentCache());
                    aggregate.totalStableSegmentWriteBufferKeyCount += Math.max(
                            0L,
                            segmentRuntime.getNumberOfKeysInWriteCache());
                    aggregate.totalStableSegmentDeltaCacheFileCount += Math.max(
                            0,
                            segmentRuntime.getNumberOfDeltaCacheFiles());
                    aggregate.stableSegmentMetricsSnapshots.add(
                            new SegmentIndexMetricsSnapshot.SegmentMetricsSnapshot(
                                    segmentRuntime));
                    aggregate.totalCompactRequestCount += Math.max(0L,
                            segmentRuntime.getNumberOfCompacts());
                    aggregate.totalFlushRequestCount += Math.max(0L,
                            segmentRuntime.getNumberOfFlushes());
                    aggregate.totalBloomFilterRequestCount += Math.max(0L,
                            segmentRuntime.getBloomFilterRequestCount());
                    aggregate.totalBloomFilterRefusedCount += Math.max(0L,
                            segmentRuntime.getBloomFilterRefusedCount());
                    aggregate.totalBloomFilterPositiveCount += Math.max(0L,
                            segmentRuntime.getBloomFilterPositiveCount());
                    aggregate.totalBloomFilterFalsePositiveCount += Math.max(
                            0L,
                            segmentRuntime.getBloomFilterFalsePositiveCount());
                }
            }
        }
        aggregate.unloadedMappedStableSegmentCount = Math.max(0,
                aggregate.totalMappedStableSegmentCount - accountedSegments);
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
        final int activePartition = effective.get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION)
                .intValue();
        final int partitionBuffer = effective.get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER)
                .intValue();
        final int indexBuffer = effective
                .get(RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER)
                .intValue();
        if (partitionBuffer <= activePartition) {
            issues.add(new ValidationIssue(
                    RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
                    "value must be greater than maxNumberOfKeysInActivePartition"));
        }
        if (indexBuffer < partitionBuffer) {
            issues.add(new ValidationIssue(
                    RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER,
                    "value must be >= maxNumberOfKeysInPartitionBuffer"));
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
        if (validation.normalizedValues().containsKey(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT)) {
            scheduleBackgroundSplitPolicyScan();
        }
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
        final int maxActivePartition = effective.get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION)
                .intValue();
        final int maxPartitionBuffer = effective.get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER)
                .intValue();
        segmentFactory.updateRuntimeLimits(maxSegmentCache, maxActivePartition,
                maxPartitionBuffer);
        final SegmentRuntimeLimits limits = new SegmentRuntimeLimits(
                maxSegmentCache, maxActivePartition, maxPartitionBuffer);
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
                    RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION,
                    configuration.getMaxNumberOfKeysInActivePartition());
            baselineValues.put(
                    RuntimeSettingKey.MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION,
                    configuration.getMaxNumberOfImmutableRunsPerPartition());
            baselineValues.put(
                    RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
                    configuration.getMaxNumberOfKeysInPartitionBuffer());
            baselineValues.put(
                    RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER,
                    configuration.getMaxNumberOfKeysInIndexBuffer());
            baselineValues.put(
                    RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
                    configuration.getMaxNumberOfKeysInPartitionBeforeSplit());
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

    private static final class StableSegmentRuntimeAggregate {
        private int totalMappedStableSegmentCount;
        private int readyStableSegmentCount;
        private int stableSegmentsInMaintenanceStateCount;
        private int errorStableSegmentCount;
        private int closedStableSegmentCount;
        private int unloadedMappedStableSegmentCount;
        private long totalStableSegmentKeyCount;
        private long totalStableSegmentCacheKeyCount;
        private long totalStableSegmentWriteBufferKeyCount;
        private long totalStableSegmentDeltaCacheFileCount;
        private long totalCompactRequestCount;
        private long totalFlushRequestCount;
        private long totalBloomFilterRequestCount;
        private long totalBloomFilterRefusedCount;
        private long totalBloomFilterPositiveCount;
        private long totalBloomFilterFalsePositiveCount;
        private final List<SegmentIndexMetricsSnapshot.SegmentMetricsSnapshot> stableSegmentMetricsSnapshots = new ArrayList<>();
    }

    /** {@inheritDoc} */
    @Override
    public IndexConfiguration<K, V> getConfiguration() {
        return conf;
    }

}
