package org.hestiastore.index.segmentindex.core;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.facade.SegmentIndexMaintenanceCommands;
import org.hestiastore.index.segmentindex.core.facade.SegmentIndexMutationFacade;
import org.hestiastore.index.segmentindex.core.facade.SegmentIndexReadFacade;
import org.hestiastore.index.segmentindex.core.infrastructure.IndexExecutorRegistry;
import org.hestiastore.index.segmentindex.core.internal.IndexInternal;
import org.hestiastore.index.segmentindex.core.runtime.SegmentIndexRuntime;
import org.hestiastore.index.segmentindex.core.state.IndexState;
import org.hestiastore.index.segmentindex.core.state.IndexStateCoordinator;
import org.hestiastore.index.segmentindex.core.state.IndexStateOpening;
import org.hestiastore.index.sorteddatafile.EntryComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base implementation of the segment index that manages routed segment
 * storage, WAL coordination, and background maintenance.
 *
 * @param <K> key type
 * @param <V> value type
 */
public abstract class SegmentIndexImpl<K, V> extends AbstractCloseableResource
        implements IndexInternal<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final IndexConfiguration<K, V> conf;
    protected final TypeDescriptor<K> keyTypeDescriptor;
    private final SegmentIndexMutationFacade<K, V> mutationFacade;
    private final SegmentIndexReadFacade<K, V> readFacade;
    private final SegmentIndexMaintenanceCommands<K, V> maintenanceCommands;
    private final SegmentIndexCoreOwner<K, V> coreOwner;

    protected SegmentIndexImpl(final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final IndexRuntimeConfiguration<K, V> runtimeConfiguration,
            final IndexExecutorRegistry executorRegistry) {
        IndexStateCoordinator<K, V> initializedStateCoordinator = null;
        try {
            final IndexConfiguration<K, V> validatedConfiguration = Vldtn
                    .requireNonNull(conf, "conf");
            final TypeDescriptor<K> validatedKeyTypeDescriptor = Vldtn
                    .requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
            final IndexStateOpening<K, V> openingState = new IndexStateOpening<>(
                    directoryFacade);
            final IndexStateCoordinator<K, V> createdStateCoordinator =
                    new IndexStateCoordinator<>(openingState,
                            SegmentIndexState.OPENING);
            initializedStateCoordinator = createdStateCoordinator;
            final SegmentIndexCoreGraph<K, V> composition =
                    SegmentIndexCoreGraph.create(
                            SegmentIndexCoreInputs.create(logger,
                                    directoryFacade,
                                    validatedKeyTypeDescriptor,
                                    valueTypeDescriptor,
                                    validatedConfiguration,
                                    runtimeConfiguration, executorRegistry,
                                    createdStateCoordinator,
                                    openingState.wasStaleLockRecovered()));
            this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                    "keyTypeDescriptor");
            this.conf = validatedConfiguration;
            this.mutationFacade = composition.mutationFacade();
            this.readFacade = composition.readFacade();
            this.maintenanceCommands = composition.maintenanceCommands();
            this.coreOwner = new SegmentIndexCoreOwner<>(validatedConfiguration,
                    createdStateCoordinator, composition.runtime(),
                    composition.maintenanceAccess(),
                    composition.closeCoordinator(),
                    composition.startupCoordinator());
        } catch (final RuntimeException e) {
            if (initializedStateCoordinator != null) {
                initializedStateCoordinator.failWithError(e);
            }
            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void put(final K key, final V value) {
        mutationFacade.put(key, value);
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
    public void compact() {
        runMaintenanceOperation(maintenanceCommands::compact);
    }

    /** {@inheritDoc} */
    @Override
    public void compactAndWait() {
        runMaintenanceOperation(maintenanceCommands::compactAndWait);
    }

    /** {@inheritDoc} */
    @Override
    public V get(final K key) {
        return mutationFacade.get(key);
    }

    /** {@inheritDoc} */
    @Override
    public void delete(final K key) {
        mutationFacade.delete(key);
    }

    /** {@inheritDoc} */
    @Override
    public void checkAndRepairConsistency() {
        runMaintenanceOperation(maintenanceCommands::checkAndRepairConsistency);
    }

    /** {@inheritDoc} */
    @Override
    protected void doClose() {
        coreOwner.close();
    }

    public final IndexState<K, V> getIndexState() {
        return coreOwner.getIndexState();
    }

    /** {@inheritDoc} */
    @Override
    public SegmentIndexState getState() {
        return coreOwner.getState();
    }

    /** {@inheritDoc} */
    @Override
    public SegmentIndexMetricsSnapshot metricsSnapshot() {
        return coreOwner.metricsSnapshot();
    }

    protected final void failWithError(final Throwable failure) {
        coreOwner.failWithError(failure);
    }

    /**
     * Startup-only extension point invoked immediately before the first
     * consistency repair that runs after stale lock recovery.
     */
    protected void onStartupConsistencyCheck() {
    }

    protected final void completeStartup() {
        coreOwner.completeStartup(this::onStartupConsistencyCheck);
    }

    /** {@inheritDoc} */
    @Override
    public void flush() {
        runMaintenanceOperation(maintenanceCommands::flush);
    }

    /** {@inheritDoc} */
    @Override
    public void flushAndWait() {
        runMaintenanceOperation(maintenanceCommands::flushAndWait);
    }

    protected void ensureOperational() {
        coreOwner.ensureOperational();
    }

    protected void runMaintenanceOperation(final Runnable action) {
        coreOwner.runMaintenanceOperation(action);
    }

    protected final IndexStateCoordinator<K, V> stateCoordinator() {
        return coreOwner.stateCoordinator();
    }

    final SegmentIndexRuntime<K, V> runtime() {
        return coreOwner.runtime();
    }

    /** {@inheritDoc} */
    @Override
    public IndexControlPlane controlPlane() {
        return coreOwner.controlPlane();
    }

    /** {@inheritDoc} */
    @Override
    public IndexConfiguration<K, V> getConfiguration() {
        return conf;
    }

}
