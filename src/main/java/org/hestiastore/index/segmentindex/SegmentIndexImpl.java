package org.hestiastore.index.segmentindex;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.F;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentregistry.SegmentFactory;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryImpl;
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
abstract class SegmentIndexImpl<K, V> extends AbstractCloseableResource
        implements IndexInternal<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final IndexConfiguration<K, V> conf;
    protected final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentMaintenanceCoordinator<K, V> maintenanceCoordinator;
    private final SegmentAsyncExecutor segmentAsyncExecutor;
    private final SplitAsyncExecutor splitAsyncExecutor;
    private final SegmentIndexCore<K, V> core;
    private final IndexRetryPolicy retryPolicy;
    private final Stats stats = new Stats();
    private final Object asyncMonitor = new Object();
    private int asyncInFlight = 0;
    private final ThreadLocal<Boolean> inAsyncOperation = ThreadLocal
            .withInitial(() -> Boolean.FALSE);
    private volatile IndexState<K, V> indexState;
    private volatile SegmentIndexState segmentIndexState = SegmentIndexState.OPENING;

    protected SegmentIndexImpl(final AsyncDirectory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf) {
        Vldtn.requireNonNull(directoryFacade, "directoryFacade");
        setIndexState(new IndexStateOpening<>(directoryFacade));
        setSegmentIndexState(SegmentIndexState.OPENING);
        try {
            this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                    "keyTypeDescriptor");
            this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                    "valueTypeDescriptor");
            this.conf = Vldtn.requireNonNull(conf, "conf");
            final KeyToSegmentMap<K> keyToSegmentMapDelegate = new KeyToSegmentMap<>(
                    directoryFacade, keyTypeDescriptor);
            this.keyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                    keyToSegmentMapDelegate);
            final Integer maintenanceThreadsConf = conf
                    .getNumberOfSegmentIndexMaintenanceThreads();
            final int maintenanceThreads = maintenanceThreadsConf.intValue();
            this.segmentAsyncExecutor = new SegmentAsyncExecutor(
                    maintenanceThreads, "segment-maintenance");
            final Integer splitThreadsConf = conf
                    .getNumberOfIndexMaintenanceThreads();
            final int splitThreads = splitThreadsConf.intValue();
            this.splitAsyncExecutor = new SplitAsyncExecutor(splitThreads,
                    "index-maintenance");
            final SegmentFactory<K, V> segmentFactory = new SegmentFactory<>(
                    directoryFacade, keyTypeDescriptor, valueTypeDescriptor,
                    conf, segmentAsyncExecutor.getExecutor());
            final SegmentRegistryImpl<K, V> registry = SegmentRegistry
                    .<K, V>builder()
                    .withDirectoryFacade(directoryFacade)
                    .withKeyTypeDescriptor(keyTypeDescriptor)
                    .withValueTypeDescriptor(valueTypeDescriptor)
                    .withConfiguration(conf)
                    .withMaintenanceExecutor(segmentAsyncExecutor.getExecutor())
                    .withSegmentFactory(segmentFactory)
                    .build();
            this.segmentRegistry = registry;
            final SegmentRegistryAccess<K, V> registryAccess = new SegmentRegistryAccessAdapter<>(
                    registry);
            final SegmentWriterTxFactory<K, V> writerTxFactory = id -> segmentFactory
                    .newSegmentBuilder(id).openWriterTx();
            final SegmentSplitCoordinator<K, V> splitCoordinator = new SegmentSplitCoordinator<>(
                    conf, keyToSegmentMap, segmentRegistry, registryAccess,
                    writerTxFactory);
            final SegmentAsyncSplitCoordinator<K, V> asyncSplitCoordinator = new SegmentAsyncSplitCoordinator<>(
                    splitCoordinator, splitAsyncExecutor.getExecutor());
            this.maintenanceCoordinator = new SegmentMaintenanceCoordinator<>(
                    conf, keyToSegmentMap, registryAccess,
                    asyncSplitCoordinator);
            this.core = new SegmentIndexCore<>(keyToSegmentMap, segmentRegistry,
                    maintenanceCoordinator);
            this.retryPolicy = new IndexRetryPolicy(
                    conf.getIndexBusyBackoffMillis(),
                    conf.getIndexBusyTimeoutMillis());
            getIndexState().onReady(this);
            setSegmentIndexState(SegmentIndexState.READY);
        } catch (final RuntimeException e) {
            failWithError(e);
            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void put(final K key, final V value) {
        getIndexState().tryPerformOperation();
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        stats.incPutCx();

        if (valueTypeDescriptor.isTombstone(value)) {
            throw new IllegalArgumentException(String.format(
                    "Can't insert thombstone value '%s' into index", value));
        }

        final IndexResult<Void> result = retryWhileBusyWithSplitWait(
                () -> core.put(key, value), "put", key, true);
        if (result.getStatus() == IndexResultStatus.OK) {
            return;
        }
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

    /** {@inheritDoc} */
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
        final EntryIterator<K, V> segmentIterator = new SegmentsIterator<>(
                keyToSegmentMap.getSegmentIds(resolvedWindows), segmentRegistry,
                isolation);
        if (conf.isContextLoggingEnabled()) {
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
        getIndexState().tryPerformOperation();
        Vldtn.requireNonNull(key, "key");
        stats.incGetCx();

        final IndexResult<V> result = retryWhileBusyWithSplitWait(
                () -> core.get(key), "get", key, true);
        if (result.getStatus() == IndexResultStatus.OK) {
            return result.getValue();
        }
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
        getIndexState().tryPerformOperation();
        Vldtn.requireNonNull(key, "key");
        stats.incDeleteCx();
        final IndexResult<Void> result = retryWhileBusyWithSplitWait(
                () -> core.put(key, valueTypeDescriptor.getTombstone()),
                "delete", key, true);
        if (result.getStatus() == IndexResultStatus.OK) {
            return;
        }
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
        getIndexState().onClose(this);
        setSegmentIndexState(SegmentIndexState.CLOSED);
        awaitAsyncOperations();
        flushSegments(true);
        segmentRegistry.close();
        keyToSegmentMap.optionalyFlush();
        if (!segmentAsyncExecutor.wasClosed()) {
            segmentAsyncExecutor.close();
        }
        if (!splitAsyncExecutor.wasClosed()) {
            splitAsyncExecutor.close();
        }
        if (logger.isDebugEnabled()) {
            logger.debug(String.format(
                    "Index is closing, where was %s gets, %s puts and %s deletes.",
                    F.fmt(stats.getGetCx()), F.fmt(stats.getPutCx()),
                    F.fmt(stats.getDeleteCx())));
        }
    }

    private <T> CompletionStage<T> runAsyncTracked(final Supplier<T> task) {
        incrementAsync();
        try {
            return CompletableFuture.supplyAsync(() -> {
                final boolean previous = Boolean.TRUE
                        .equals(inAsyncOperation.get());
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
        boolean interrupted = false;
        synchronized (asyncMonitor) {
            while (asyncInFlight > 0) {
                try {
                    asyncMonitor.wait();
                } catch (final InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
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
                        awaitSegmentReady(segmentId, "compact", segment);
                    }
                }
                return;
            }
            if (status == IndexResultStatus.CLOSED) {
                return;
            }
            if (status == IndexResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "compact", segmentId);
                continue;
            }
            throw newIndexException("compact", segmentId, status);
        }
    }

    private void flushSegment(final SegmentId segmentId,
            final boolean waitForCompletion) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final IndexResult<Segment<K, V>> result = core.flush(segmentId);
            final IndexResultStatus status = result.getStatus();
            if (status == IndexResultStatus.OK) {
                if (waitForCompletion) {
                    final Segment<K, V> segment = result.getValue();
                    if (segment != null) {
                        awaitSegmentReady(segmentId, "flush", segment);
                    }
                }
                return;
            }
            if (status == IndexResultStatus.CLOSED) {
                return;
            }
            if (status == IndexResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "flush", segmentId);
                continue;
            }
            throw newIndexException("flush", segmentId, status);
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
            final long startNanos = retryPolicy.startNanos();
            while (true) {
                final SegmentRegistryResult<Segment<K, V>> segmentResult = segmentRegistry
                        .getSegment(segmentId);
                if (segmentResult
                        .getStatus() == SegmentRegistryResultStatus.OK) {
                    segmentResult.getValue().invalidateIterators();
                    return;
                }
                if (segmentResult
                        .getStatus() == SegmentRegistryResultStatus.BUSY) {
                    retryPolicy.backoffOrThrow(startNanos,
                            "invalidateIterators", segmentId);
                    continue;
                }
                throw new IndexException(
                        String.format("Segment '%s' failed to load: %s",
                                segmentId, segmentResult.getStatus()));
            }
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

    /** {@inheritDoc} */
    @Override
    public IndexConfiguration<K, V> getConfiguration() {
        return conf;
    }

}
