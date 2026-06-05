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
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;
import org.hestiastore.index.sorteddatafile.EntryComparator;

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

    SegmentIndexImpl(final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentIndexPointOperationFacade<K, V> pointOperationFacade,
            final SegmentIndexReadFacade<K, V> readFacade,
            final SegmentIndexMaintenance maintenanceApi,
            final SegmentIndexSessionOwner<K, V> sessionOwner) {
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.pointOperationFacade = Vldtn.requireNonNull(pointOperationFacade,
                "pointOperationFacade");
        this.readFacade = Vldtn.requireNonNull(readFacade, "readFacade");
        this.maintenanceApi = Vldtn.requireNonNull(maintenanceApi,
                "maintenanceApi");
        this.sessionOwner = Vldtn.requireNonNull(sessionOwner, "sessionOwner");
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

    void ensureOperational() {
        sessionOwner.ensureOperational();
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
