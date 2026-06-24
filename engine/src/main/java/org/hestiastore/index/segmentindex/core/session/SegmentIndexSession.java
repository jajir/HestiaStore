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
import org.hestiastore.index.segmentindex.MemoryEstimateReport;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.execution.EntryIteratorLoggingContext;
import org.hestiastore.index.segmentindex.core.execution.PointOperationCoordinator;
import org.hestiastore.index.segmentindex.core.execution.SegmentIteratorService;
import org.hestiastore.index.segmentindex.SegmentIndexMaintenance;
import org.hestiastore.index.segmentindex.monitoring.SegmentIndexRuntimeMonitoring;
import org.hestiastore.index.sorteddatafile.EntryComparator;

/**
 * Base implementation of the segment index that manages routed segment
 * storage, WAL coordination, and background maintenance.
 *
 * @param <K> key type
 * @param <V> value type
 */
class SegmentIndexSession<K, V> extends AbstractCloseableResource
        implements SegmentIndex<K, V> {

    private final TypeDescriptor<K> keyTypeDescriptor;
    private final SessionOperationGate operationGate;
    private final PointOperationCoordinator<K, V> operationAccess;
    private final SegmentIteratorService<K, V> streamingService;
    private final EffectiveIndexConfiguration<K, V> configuration;
    private final RuntimeTuning runtimeTuning;
    private final SegmentIndexRuntimeMonitoring runtimeMonitoring;
    private final MemoryEstimateReport startupMemoryEstimate;
    private final SegmentIndexMaintenance maintenanceApi;
    private final SegmentIndexStateMachine stateMachine;
    private final SessionCloseCoordinator<K, V> closeCoordinator;

    /**
     * Creates the session index API from already assembled runtime,
     * maintenance, and lifecycle collaborators.
     *
     * @param keyTypeDescriptor key type descriptor used for stream ordering
     * @param operationGate foreground operation tracking gate
     * @param operationAccess foreground point operation access
     * @param streamingService segment streaming service
     * @param configuration configuration used for optional iterator context
     *            logging
     * @param runtimeTuning runtime tuning API view
     * @param runtimeMonitoring runtime monitoring API view
     * @param startupMemoryEstimate startup memory estimate captured during bootstrap
     * @param maintenanceApi maintenance API view
     * @param stateMachine session lifecycle state machine
     * @param closeCoordinator ordered close sequence coordinator
     */
    SegmentIndexSession(final TypeDescriptor<K> keyTypeDescriptor,
            final SessionOperationGate operationGate,
            final PointOperationCoordinator<K, V> operationAccess,
            final SegmentIteratorService<K, V> streamingService,
            final EffectiveIndexConfiguration<K, V> configuration,
            final RuntimeTuning runtimeTuning,
            final SegmentIndexRuntimeMonitoring runtimeMonitoring,
            final MemoryEstimateReport startupMemoryEstimate,
            final SegmentIndexMaintenance maintenanceApi,
            final SegmentIndexStateMachine stateMachine,
            final SessionCloseCoordinator<K, V> closeCoordinator) {
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.operationGate = Vldtn.requireNonNull(operationGate,
                "operationGate");
        this.operationAccess = Vldtn.requireNonNull(operationAccess,
                "operationAccess");
        this.streamingService = Vldtn.requireNonNull(streamingService,
                "streamingService");
        this.configuration = Vldtn.requireNonNull(configuration,
                "configuration");
        this.runtimeTuning = Vldtn.requireNonNull(runtimeTuning,
                "runtimeTuning");
        this.runtimeMonitoring = Vldtn.requireNonNull(runtimeMonitoring,
                "runtimeMonitoring");
        this.startupMemoryEstimate = Vldtn.requireNonNull(startupMemoryEstimate,
                "startupMemoryEstimate");
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
        beginOperationalOperation();
        try {
            operationAccess.put(key, value);
        } finally {
            operationGate.endOperation();
        }
    }

    /**
     * Opens an iterator for one segment with fail-fast isolation.
     *
     * @param segmentId required segment id
     * @return segment iterator
     */
    EntryIterator<K, V> openSegmentIterator(final SegmentId segmentId) {
        return openSegmentIterator(segmentId,
                SegmentIteratorIsolation.FAIL_FAST);
    }

    EntryIterator<K, V> openSegmentIterator(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        beginOperationalOperation();
        try {
            return streamingService.openIterator(
                    requireSegmentId(segmentId),
                    requireIsolation(isolation));
        } finally {
            operationGate.endOperation();
        }
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
        beginOperationalOperation();
        try {
            return decorateIterator(streamingService.openWindowIterator(
                    resolveSegmentWindow(segmentWindows),
                    requireIsolation(isolation)));
        } finally {
            operationGate.endOperation();
        }
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
        beginOperationalOperation();
        try {
            return operationAccess.get(key);
        } finally {
            operationGate.endOperation();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void delete(final K key) {
        beginOperationalOperation();
        try {
            operationAccess.delete(key);
        } finally {
            operationGate.endOperation();
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void doClose() {
        closeCoordinator.close();
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
    public SegmentIndexRuntimeMonitoring runtimeMonitoring() {
        return runtimeMonitoring;
    }

    /** {@inheritDoc} */
    @Override
    public MemoryEstimateReport startupMemoryEstimate() {
        return startupMemoryEstimate;
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

    private void beginOperationalOperation() {
        operationGate.beginOperation();
        try {
            stateMachine.ensureOperational();
        } catch (final RuntimeException e) {
            operationGate.endOperation();
            throw e;
        }
    }

}
