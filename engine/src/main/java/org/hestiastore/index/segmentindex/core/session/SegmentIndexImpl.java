package org.hestiastore.index.segmentindex.core.session;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.storage.IndexConsistencyCoordinator;
import org.hestiastore.index.segmentindex.core.streaming.SegmentIndexReadFacade;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenanceImpl;
import org.hestiastore.index.segmentindex.metrics.Stats;
import org.hestiastore.index.sorteddatafile.EntryComparator;
import org.slf4j.Logger;

/**
 * Base implementation of the segment index that manages routed segment
 * storage, WAL coordination, and background maintenance.
 *
 * @param <K> key type
 * @param <V> value type
 */
class SegmentIndexImpl<K, V> extends AbstractCloseableResource
        implements IndexInternal<K, V> {

    private final TypeDescriptor<K> keyTypeDescriptor;
    private final SegmentIndexPointOperationFacade<K, V> pointOperationFacade;
    private final SegmentIndexReadFacade<K, V> readFacade;
    private final SegmentIndexMaintenance maintenanceApi;
    private final SegmentIndexSessionOwner<K, V> sessionOwner;

    @SuppressWarnings("java:S107")
    SegmentIndexImpl(final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentIndexPointOperationFacade<K, V> pointOperationFacade,
            final SegmentIndexReadFacade<K, V> readFacade,
            final MaintenanceService maintenance,
            final SegmentIndexTrackedOperationRunner<K, V> trackedRunner,
            final SegmentIndexMaintenance maintenanceApi,
            final SegmentIndexSessionOwner<K, V> sessionOwner) {
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.pointOperationFacade = Vldtn.requireNonNull(pointOperationFacade,
                "pointOperationFacade");
        this.readFacade = Vldtn.requireNonNull(readFacade, "readFacade");
        Vldtn.requireNonNull(maintenance, "maintenance");
        Vldtn.requireNonNull(trackedRunner, "trackedRunner");
        this.maintenanceApi = Vldtn.requireNonNull(maintenanceApi,
                "maintenanceApi");
        this.sessionOwner = Vldtn.requireNonNull(sessionOwner, "sessionOwner");
    }

    @SuppressWarnings("java:S107")
    static <K, V> SegmentIndexImpl<K, V> open(final Logger logger,
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final EffectiveIndexConfiguration<K, V> conf,
            final ExecutorRegistry executorRegistry) {
        final Logger validatedLogger = Vldtn.requireNonNull(logger, "logger");
        final Directory validatedDirectory = Vldtn.requireNonNull(
                directoryFacade, "directoryFacade");
        final TypeDescriptor<K> validatedKeyTypeDescriptor = Vldtn
                .requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        final TypeDescriptor<V> validatedValueTypeDescriptor = Vldtn
                .requireNonNull(valueTypeDescriptor, "valueTypeDescriptor");
        final EffectiveIndexConfiguration<K, V> validatedConf = Vldtn
                .requireNonNull(conf, "conf");
        final ExecutorRegistry validatedExecutorRegistry = Vldtn
                .requireNonNull(executorRegistry, "executorRegistry");
        SegmentIndexStateMachine stateMachine = null;
        SegmentIndexRuntime<K, V> runtime = null;
        IndexDirectoryLock directoryLock = null;
        try {
            directoryLock = new IndexDirectoryLock(validatedDirectory);
            stateMachine = new SegmentIndexStateMachine();
            final Stats stats = new Stats();
            final IndexOperationTrackingAccess operationTracker =
                    IndexOperationTrackingAccess.create();
            final SegmentIndexTrackedOperationRunner<K, V> trackedRunner =
                    new SegmentIndexTrackedOperationRunner<>(
                            stateMachine::ensureOperational, operationTracker);
            runtime = SegmentIndexRuntime.create(validatedLogger,
                    validatedDirectory, validatedKeyTypeDescriptor,
                    validatedValueTypeDescriptor, validatedConf,
                    validatedExecutorRegistry, stats, stateMachine::getState,
                    stateMachine::markRuntimeFailure);
            final IndexConsistencyCoordinator<K, V> consistencyCoordinator =
                    newConsistencyCoordinator(runtime);
            final SegmentIndexFacades<K, V> facades = SegmentIndexFacades
                    .create(validatedConf, trackedRunner, runtime);
            final SegmentIndexSessionOwner<K, V> sessionOwner = newSessionOwner(
                    validatedLogger, validatedConf, stateMachine, runtime,
                    operationTracker, stats, directoryLock,
                    consistencyCoordinator);
            final SegmentIndexMaintenance maintenanceApi = newMaintenanceApi(
                    sessionOwner, trackedRunner, runtime.maintenance(),
                    consistencyCoordinator);
            return new SegmentIndexImpl<>(validatedKeyTypeDescriptor,
                    facades.pointOperationFacade(), facades.readFacade(),
                    runtime.maintenance(), trackedRunner, maintenanceApi,
                    sessionOwner);
        } catch (final RuntimeException failure) {
            if (runtime != null) {
                runtime.closeForFailedStartup(failure);
            }
            closeDirectoryLock(directoryLock, failure);
            throw failure;
        }
    }

    private static void closeDirectoryLock(final IndexDirectoryLock directoryLock,
            final RuntimeException failure) {
        if (directoryLock == null) {
            return;
        }
        try {
            directoryLock.close();
        } catch (final RuntimeException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    private static <K, V> IndexConsistencyCoordinator<K, V> newConsistencyCoordinator(
            final SegmentIndexRuntime<K, V> runtime) {
        final SegmentIndexRuntime<K, V> validatedRuntime = Vldtn
                .requireNonNull(runtime, "runtime");
        return new IndexConsistencyCoordinator<>(
                validatedRuntime::validateUniqueSegmentIds,
                validatedRuntime::checkAndRepairConsistency,
                validatedRuntime::cleanupOrphanedSegmentDirectories,
                validatedRuntime::requestFullSplitScan,
                validatedRuntime::hasSegmentLockFile);
    }

    private static <K, V> SegmentIndexMaintenance newMaintenanceApi(
            final SegmentIndexSessionOwner<K, V> sessionOwner,
            final SegmentIndexTrackedOperationRunner<K, V> trackedRunner,
            final MaintenanceService maintenance,
            final IndexConsistencyCoordinator<K, V> consistencyCoordinator) {
        return new SegmentIndexMaintenanceImpl(
                () -> runTrackedMaintenanceOperation(sessionOwner, trackedRunner,
                        maintenance::compact),
                () -> runTrackedMaintenanceOperation(sessionOwner, trackedRunner,
                        maintenance::compactAndWait),
                () -> runTrackedMaintenanceOperation(sessionOwner, trackedRunner,
                        maintenance::flush),
                () -> runTrackedMaintenanceOperation(sessionOwner, trackedRunner,
                        maintenance::flushAndWait),
                () -> runTrackedMaintenanceOperation(sessionOwner, trackedRunner,
                        consistencyCoordinator::checkAndRepairConsistency));
    }

    private static <K, V> void runTrackedMaintenanceOperation(
            final SegmentIndexSessionOwner<K, V> sessionOwner,
            final SegmentIndexTrackedOperationRunner<K, V> trackedRunner,
            final Runnable action) {
        sessionOwner.runMaintenanceOperation(
                () -> trackedRunner.runTrackedVoid(action));
    }

    private static <K, V> SegmentIndexSessionOwner<K, V> newSessionOwner(
            final Logger logger, final EffectiveIndexConfiguration<K, V> conf,
            final SegmentIndexStateMachine stateMachine,
            final SegmentIndexRuntime<K, V> runtime,
            final IndexOperationTrackingAccess operationTracker,
            final Stats stats, final IndexDirectoryLock directoryLock,
            final IndexConsistencyCoordinator<K, V> consistencyCoordinator) {
        return new SegmentIndexSessionOwner<>(stateMachine, runtime,
                new IndexCloseCoordinator<>(logger, conf.identity().name(),
                        stateMachine, operationTracker, stats, runtime,
                        directoryLock),
                new SegmentIndexStartupCoordinator<>(logger,
                        conf.identity().name(),
                        directoryLock.wasStaleLockRecovered(), runtime,
                        stateMachine, consistencyCoordinator));
    }

    /** {@inheritDoc} */
    @Override
    public void put(final K key, final V value) {
        pointOperationFacade.put(key, value);
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
        return readFacade.openSegmentIterator(segmentId, isolation);
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
        return readFacade.openWindowIterator(segmentWindows, isolation);
    }

    /** {@inheritDoc} */
    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindow) {
        return getStream(segmentWindow, SegmentIteratorIsolation.FAIL_FAST);
    }

    /** {@inheritDoc} */
    @Override
    public Stream<Entry<K, V>> getStream(
            final SegmentWindow segmentWindow,
            final SegmentIteratorIsolation isolation) {
        ensureOperational();
        final EntryIterator<K, V> iterator = openSegmentIterator(segmentWindow,
                isolation);
        return StreamSupport.stream(newEntryIteratorSpliterator(iterator), false)
                .onClose(iterator::close);
    }

    private Spliterator<Entry<K, V>> newEntryIteratorSpliterator(
            final EntryIterator<K, V> iterator) {
        final EntryIterator<K, V> validatedIterator = Vldtn
                .requireNonNull(iterator, "iterator");
        final Comparator<? super Entry<K, V>> comparator =
                new EntryComparator<>(keyTypeDescriptor.getComparator());
        return new Spliterator<>() {

            @Override
            public boolean tryAdvance(
                    final Consumer<? super Entry<K, V>> action) {
                if (validatedIterator.hasNext()) {
                    action.accept(validatedIterator.next());
                    return true;
                }
                return false;
            }

            @Override
            public Spliterator<Entry<K, V>> trySplit() {
                return null;
            }

            @Override
            public long estimateSize() {
                return Integer.MAX_VALUE;
            }

            @Override
            public int characteristics() {
                return Spliterator.DISTINCT | Spliterator.IMMUTABLE
                        | Spliterator.NONNULL | Spliterator.SORTED;
            }

            @Override
            public Comparator<? super Entry<K, V>> getComparator() {
                return comparator;
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public V get(final K key) {
        return pointOperationFacade.get(key);
    }

    /** {@inheritDoc} */
    @Override
    public void delete(final K key) {
        pointOperationFacade.delete(key);
    }

    /** {@inheritDoc} */
    @Override
    protected void doClose() {
        sessionOwner.close();
    }

    final void prepareFailedStartupCleanup(final Throwable failure) {
        sessionOwner.prepareFailedStartupCleanup(failure);
    }

    /**
     * Startup-only extension point invoked immediately before the first
     * consistency repair that runs after stale lock recovery.
     */
    void onStartupConsistencyCheck() {
    }

    final void completeStartup() {
        sessionOwner.completeStartup(this::onStartupConsistencyCheck);
    }

    void ensureOperational() {
        sessionOwner.ensureOperational();
    }

    void runMaintenanceOperation(final Runnable action) {
        sessionOwner.runMaintenanceOperation(action);
    }

    @Override
    public SegmentIndexMaintenance maintenance() {
        return maintenanceApi;
    }

    final SegmentIndexStateMachine stateMachine() {
        return sessionOwner.stateMachine();
    }

    final SegmentIndexRuntime<K, V> runtime() {
        return sessionOwner.runtime();
    }

    /** {@inheritDoc} */
    @Override
    public RuntimeTuning runtimeTuning() {
        return sessionOwner.runtimeTuning();
    }

    /** {@inheritDoc} */
    @Override
    public IndexRuntimeMonitoring runtimeMonitoring() {
        return sessionOwner.runtimeMonitoring();
    }

}
