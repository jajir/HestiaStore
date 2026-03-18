package org.hestiastore.index.segmentindex.core;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.F;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentDirectoryLayout;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.partition.PartitionRuntime;
import org.hestiastore.index.segmentindex.partition.PartitionRuntimeSnapshot;
import org.hestiastore.index.segmentindex.split.PartitionStableSplitCoordinator;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentFactory;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
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

    private static final String OPERATION_DRAIN = "drain";
    static final String OPERATION_OPEN_FULL_ISOLATION_ITERATOR = "openFullIsolationIterator";
    private static final String OPERATION_CLEANUP_ORPHAN_SEGMENT = "cleanupOrphanSegment";
    private static final String INDEX_NAME_MDC_KEY = "index.name";
    private static final int DEFAULT_MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION = 2;
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
    private final BackgroundSplitPolicyLoop<K, V> backgroundSplitPolicyLoop;
    private final SegmentIndexCore<K, V> core;
    private final StableSegmentCoordinator<K, V> stableSegmentCoordinator;
    private final PartitionDrainCoordinator<K, V> partitionDrainCoordinator;
    private final PartitionWriteCoordinator<K, V> partitionWriteCoordinator;
    private final PartitionReadCoordinator<K, V> partitionReadCoordinator;
    private final IndexMaintenanceCoordinator<K, V> maintenanceCoordinator;
    private final PartitionRuntime<K, V> partitionRuntime;
    private final Executor drainExecutor;
    private final IndexRetryPolicy retryPolicy;
    private final RuntimeTuningState runtimeTuningState;
    private final WalRuntime<K, V> walRuntime;
    private final Stats stats = new Stats();
    private final SegmentIndexMetricsCollector<K, V> metricsCollector;
    private final IndexWalCoordinator<K, V> walCoordinator;
    private final IndexControlPlane controlPlane;
    private final AtomicLong compactRequestHighWaterMark = new AtomicLong();
    private final AtomicLong flushRequestHighWaterMark = new AtomicLong();
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
                final PartitionRuntime<K, V> runtime = new PartitionRuntime<>(
                        keyTypeDescriptor.getComparator());
                this.partitionRuntime = runtime;
                final PartitionStableSplitCoordinator<K, V> splitCoordinator = new PartitionStableSplitCoordinator<>(
                        conf, keyTypeDescriptor.getComparator(), keyToSegmentMap, segmentRegistry,
                        runtime);
                this.backgroundSplitCoordinator = new BackgroundSplitCoordinator<>(
                        keyToSegmentMap, runtime, splitCoordinator,
                        executorRegistry.getSplitMaintenanceExecutor(),
                        this::failWithError,
                        this::onBackgroundSplitApplied);
                this.core = new SegmentIndexCore<>(keyToSegmentMap,
                        segmentRegistry);
                this.drainExecutor = executorRegistry
                        .getIndexMaintenanceExecutor();
                this.retryPolicy = new IndexRetryPolicy(
                        conf.getIndexBusyBackoffMillis(),
                        conf.getIndexBusyTimeoutMillis());
                this.stableSegmentCoordinator = new StableSegmentCoordinator<>(
                        logger, keyToSegmentMap, segmentRegistry,
                        backgroundSplitCoordinator, core, retryPolicy, stats);
                this.backgroundSplitPolicyLoop = new BackgroundSplitPolicyLoop<>(
                        conf, runtimeTuningState, keyToSegmentMap,
                        segmentRegistry, partitionRuntime,
                        backgroundSplitCoordinator, drainExecutor,
                        executorRegistry.getSplitPolicyScheduler(), stats,
                        this::getState, this::awaitSplitsIdle,
                        this::failWithError);
                this.partitionDrainCoordinator = new PartitionDrainCoordinator<>(
                        partitionRuntime, keyToSegmentMap, drainExecutor,
                        retryPolicy, stableSegmentCoordinator, stats,
                        backgroundSplitPolicyLoop::scheduleHint,
                        this::failWithError);
                this.partitionWriteCoordinator = new PartitionWriteCoordinator<>(
                        keyToSegmentMap, partitionRuntime, runtimeTuningState,
                        backgroundSplitCoordinator,
                        partitionDrainCoordinator::scheduleDrain);
                this.partitionReadCoordinator = new PartitionReadCoordinator<>(
                        keyToSegmentMap, partitionRuntime, segmentRegistry,
                        core, backgroundSplitCoordinator, keyTypeDescriptor,
                        valueTypeDescriptor, retryPolicy);
                this.walRuntime = WalRuntime.open(nonNullDirectory, conf.getWal(),
                        keyTypeDescriptor, valueTypeDescriptor);
                this.walCoordinator = new IndexWalCoordinator<>(logger, conf,
                        walRuntime, retryPolicy,
                        () -> partitionDrainCoordinator.drainPartitions(true),
                        () -> {
                            stableSegmentCoordinator.flushSegments(true);
                            keyToSegmentMap.optionalyFlush();
                        }, this::getState, this::failWithError,
                        lastAppliedWalLsn);
                this.maintenanceCoordinator = new IndexMaintenanceCoordinator<>(
                        keyToSegmentMap, partitionRuntime,
                        partitionDrainCoordinator, backgroundSplitCoordinator,
                        backgroundSplitPolicyLoop, stableSegmentCoordinator,
                        walCoordinator);
                this.metricsCollector = new SegmentIndexMetricsCollector<>(conf,
                        keyToSegmentMap, segmentRegistry, partitionRuntime,
                        runtimeTuningState, walRuntime, stats,
                        compactRequestHighWaterMark,
                        flushRequestHighWaterMark, lastAppliedWalLsn,
                        this::getState);
                this.controlPlane = new IndexRuntimeControlPlane(conf,
                        runtimeTuningState, this::getState,
                        metricsCollector::metricsSnapshot,
                        this::applyRuntimeEffectiveLimits,
                        backgroundSplitPolicyLoop::scheduleScan);
                walCoordinator.recover(this::replayWalRecord);
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
                backgroundSplitPolicyLoop.scheduleScan();
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
        final long walLsn = walCoordinator.appendPut(key, value);

        final IndexResult<Void> result = retryWhileBusy(
                () -> putBuffered(key, value), "put", false);
        if (result.getStatus() == IndexResultStatus.OK) {
            walCoordinator.recordAppliedLsn(walLsn);
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
        return stableSegmentCoordinator.openIteratorWithRetry(segmentId,
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
        segmentIterator = partitionReadCoordinator.openWindowIterator(
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
        maintenanceCoordinator.compact();
    }

    /** {@inheritDoc} */
    @Override
    public void compactAndWait() {
        getIndexState().tryPerformOperation();
        maintenanceCoordinator.compactAndWait();
    }

    /** {@inheritDoc} */
    @Override
    public V get(final K key) {
        final long startedNanos = System.nanoTime();
        getIndexState().tryPerformOperation();
        Vldtn.requireNonNull(key, "key");
        stats.incGetCx();

        final IndexResult<V> result = retryWhileBusy(
                () -> partitionReadCoordinator.get(key), "get", true);
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
        final long walLsn = walCoordinator.appendDelete(key);
        final IndexResult<Void> result = retryWhileBusy(
                () -> putBuffered(key, valueTypeDescriptor.getTombstone()),
                "delete", false);
        if (result.getStatus() == IndexResultStatus.OK) {
            walCoordinator.recordAppliedLsn(walLsn);
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
        backgroundSplitPolicyLoop.scheduleScan();
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
            partitionDrainCoordinator.drainPartitions(true);
            backgroundSplitPolicyLoop.awaitExhausted();
            partitionDrainCoordinator.drainPartitions(true);
            backgroundSplitPolicyLoop.awaitExhausted();
            setSegmentIndexState(SegmentIndexState.CLOSED);
            backgroundSplitCoordinator.runWithSplitSchedulingPaused(
                    () -> stableSegmentCoordinator.flushSegments(true));
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
            walCoordinator.checkpoint();
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
        if (Boolean.TRUE.equals(inAsyncOperation.get())) {
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
        return metricsCollector.metricsSnapshot();
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

    private void onBackgroundSplitApplied() {
        if (backgroundSplitPolicyLoop != null) {
            backgroundSplitPolicyLoop.scheduleScanIfIdle();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void flush() {
        maintenanceCoordinator.flush();
    }

    /** {@inheritDoc} */
    @Override
    public void flushAndWait() {
        maintenanceCoordinator.flushAndWait();
    }

    private IndexResult<Void> putBuffered(final K key, final V value) {
        return partitionWriteCoordinator.putBuffered(key, value);
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
        walCoordinator.recordAppliedLsn(replayRecord.getLsn());
    }

    private <T> IndexResult<T> retryWhileBusy(
            final Supplier<IndexResult<T>> operation, final String opName,
            final boolean retryClosed) {
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
        stableSegmentCoordinator.invalidateIterators();
    }

    protected void awaitSplitsIdle() {
        backgroundSplitCoordinator
                .awaitSplitsIdle(conf.getIndexBusyTimeoutMillis());
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
        return controlPlane;
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

    /** {@inheritDoc} */
    @Override
    public IndexConfiguration<K, V> getConfiguration() {
        return conf;
    }

}
