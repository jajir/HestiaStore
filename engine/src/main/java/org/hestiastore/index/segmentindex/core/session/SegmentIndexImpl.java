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
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationCoordinator;
import org.hestiastore.index.segmentindex.core.streaming.EntryIteratorLoggingContext;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.sorteddatafile.EntryComparator;

/**
 * Base implementation of the segment index that manages routed segment
 * storage, WAL coordination, and background maintenance.
 *
 * @param <K> key type
 * @param <V> value type
 */
class SegmentIndexImpl<K, V> extends AbstractCloseableResource
        implements SegmentIndex<K, V> {

    private final TypeDescriptor<K> keyTypeDescriptor;
    private final SegmentIndexTrackedOperationRunner trackedRunner;
    private final IndexOperationCoordinator<K, V> operationAccess;
    private final SegmentTopologyRuntimeAccess<K, V> topologyRuntime;
    private final EffectiveIndexConfiguration<K, V> configuration;
    private final RuntimeTuning runtimeTuning;
    private final IndexRuntimeMonitoring runtimeMonitoring;
    private final SegmentIndexMaintenance maintenanceApi;
    private final SegmentIndexStateMachine stateMachine;
    private final IndexCloseCoordinator<K, V> closeCoordinator;

    /**
     * Creates the session index API from already assembled runtime,
     * maintenance, and lifecycle collaborators.
     *
     * @param keyTypeDescriptor key type descriptor used for stream ordering
     * @param trackedRunner foreground operation tracking and readiness checks
     * @param operationAccess foreground point operation access
     * @param topologyRuntime topology and iterator runtime access
     * @param configuration configuration used for optional iterator context
     *            logging
     * @param runtimeTuning runtime tuning API view
     * @param runtimeMonitoring runtime monitoring API view
     * @param maintenanceApi maintenance API view
     * @param stateMachine session lifecycle state machine
     * @param closeCoordinator ordered close sequence coordinator
     */
    SegmentIndexImpl(final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentIndexTrackedOperationRunner trackedRunner,
            final IndexOperationCoordinator<K, V> operationAccess,
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime,
            final EffectiveIndexConfiguration<K, V> configuration,
            final RuntimeTuning runtimeTuning,
            final IndexRuntimeMonitoring runtimeMonitoring,
            final SegmentIndexMaintenance maintenanceApi,
            final SegmentIndexStateMachine stateMachine,
            final IndexCloseCoordinator<K, V> closeCoordinator) {
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.trackedRunner = Vldtn.requireNonNull(trackedRunner,
                "trackedRunner");
        this.operationAccess = Vldtn.requireNonNull(operationAccess,
                "operationAccess");
        this.topologyRuntime = Vldtn.requireNonNull(topologyRuntime,
                "topologyRuntime");
        this.configuration = Vldtn.requireNonNull(configuration,
                "configuration");
        this.runtimeTuning = Vldtn.requireNonNull(runtimeTuning,
                "runtimeTuning");
        this.runtimeMonitoring = Vldtn.requireNonNull(runtimeMonitoring,
                "runtimeMonitoring");
        this.maintenanceApi = Vldtn.requireNonNull(maintenanceApi,
                "maintenanceApi");
        this.stateMachine = Vldtn.requireNonNull(stateMachine,
                "stateMachine");
        this.closeCoordinator = Vldtn.requireNonNull(closeCoordinator,
                "closeCoordinator");
    }

    /** {@inheritDoc} */
    @Override
    public void put(final K key, final V value) {
        trackedRunner.runTrackedVoid(() -> operationAccess.put(key, value));
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
        return trackedRunner.runTracked(() -> topologyRuntime
                .openSegmentIterator(requireSegmentId(segmentId),
                        requireIsolation(isolation)));
    }

    /**
     * Opens a segment iterator using the provided isolation level.
     *
     * @param segmentWindows window selecting segments to iterate
     * @param isolation      iterator isolation mode
     * @return entry iterator over the selected segments
     */
    EntryIterator<K, V> openSegmentIterator(
            final SegmentWindow segmentWindows,
            final SegmentIteratorIsolation isolation) {
        return trackedRunner.runTracked(() -> decorateIterator(
                topologyRuntime.openWindowIterator(
                        resolveSegmentWindow(segmentWindows),
                        requireIsolation(isolation))));
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
        final Comparator<? super Entry<K, V>> comparator = new EntryComparator<>(keyTypeDescriptor.getComparator());
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
                return DISTINCT | IMMUTABLE | NONNULL | SORTED;
            }

            @Override
            public Comparator<? super Entry<K, V>> getComparator() {
                return comparator;
            }
        };
    }

    private EntryIterator<K, V> decorateIterator(
            final EntryIterator<K, V> iterator) {
        final EntryIterator<K, V> validatedIterator = Vldtn
                .requireNonNull(iterator, "iterator");
        if (!configuration.logging().contextEnabled()) {
            return validatedIterator;
        }
        return new EntryIteratorLoggingContext<>(validatedIterator,
                configuration);
    }

    /** {@inheritDoc} */
    @Override
    public V get(final K key) {
        return trackedRunner.runTracked(() -> operationAccess.get(key));
    }

    /** {@inheritDoc} */
    @Override
    public void delete(final K key) {
        trackedRunner.runTrackedVoid(() -> operationAccess.delete(key));
    }

    /** {@inheritDoc} */
    @Override
    protected void doClose() {
        closeCoordinator.close();
    }

    void ensureOperational() {
        stateMachine.ensureOperational();
    }

    @Override
    public SegmentIndexMaintenance maintenance() {
        return maintenanceApi;
    }

    final SegmentIndexStateMachine stateMachine() {
        return stateMachine;
    }

    /** {@inheritDoc} */
    @Override
    public RuntimeTuning runtimeTuning() {
        return runtimeTuning;
    }

    /** {@inheritDoc} */
    @Override
    public IndexRuntimeMonitoring runtimeMonitoring() {
        return runtimeMonitoring;
    }

    private SegmentId requireSegmentId(final SegmentId segmentId) {
        return Vldtn.requireNonNull(segmentId, "segmentId");
    }

    private SegmentIteratorIsolation requireIsolation(
            final SegmentIteratorIsolation isolation) {
        return Vldtn.requireNonNull(isolation, "isolation");
    }

    private SegmentWindow resolveSegmentWindow(final SegmentWindow segmentWindow) {
        return segmentWindow == null ? SegmentWindow.unbounded()
                : segmentWindow;
    }

}
