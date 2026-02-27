package org.hestiastore.index.segmentindex.core;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentWindow;

/**
 * Direct, caller-thread implementation of {@link SegmentIndex}.
 * <p>
 * Executes synchronous API calls on the caller thread while preserving the
 * existing SegmentIndexImpl retry behavior and iterator invalidation rules.
 *
 * @param <K> key type
 * @param <V> value type
 */
class IndexInternalConcurrent<K, V> extends SegmentIndexImpl<K, V> {

    /**
     * Creates a concurrent index implementation bound to the given directory
     * and type descriptors.
     *
     * @param directoryFacade directory facade
     * @param keyTypeDescriptor key type descriptor
     * @param valueTypeDescriptor value type descriptor
     * @param conf configuration for the index
     * @param executorRegistry shared executor registry
     */
    IndexInternalConcurrent(final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final IndexExecutorRegistry executorRegistry) {
        super(directoryFacade, keyTypeDescriptor, valueTypeDescriptor, conf,
                executorRegistry);
    }

    /** {@inheritDoc} */
    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindow) {
        return getStream(segmentWindow, SegmentIteratorIsolation.FAIL_FAST);
    }

    /** {@inheritDoc} */
    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindow,
            final SegmentIteratorIsolation isolation) {
        getIndexState().tryPerformOperation();
        awaitSplitsIdle();
        final EntryIterator<K, V> iterator = openSegmentIterator(segmentWindow,
                isolation);
        final EntryIteratorToSpliterator<K, V> spliterator = new EntryIteratorToSpliterator<K, V>(
                iterator, keyTypeDescriptor);
        return StreamSupport.stream(spliterator, false)
                .onClose(iterator::close);
    }

    /** {@inheritDoc} */
    @Override
    public void flush() {
        getIndexState().tryPerformOperation();
        awaitSplitsIdle();
        invalidateSegmentIterators();
        super.flush();
    }

    /** {@inheritDoc} */
    @Override
    public void flushAndWait() {
        getIndexState().tryPerformOperation();
        awaitSplitsIdle();
        invalidateSegmentIterators();
        super.flushAndWait();
    }

    /** {@inheritDoc} */
    @Override
    public void compact() {
        getIndexState().tryPerformOperation();
        awaitSplitsIdle();
        invalidateSegmentIterators();
        super.compact();
    }

    /** {@inheritDoc} */
    @Override
    public void compactAndWait() {
        getIndexState().tryPerformOperation();
        awaitSplitsIdle();
        invalidateSegmentIterators();
        super.compactAndWait();
    }

    /** {@inheritDoc} */
    @Override
    public void checkAndRepairConsistency() {
        getIndexState().tryPerformOperation();
        awaitSplitsIdle();
        invalidateSegmentIterators();
        super.checkAndRepairConsistency();
    }
}
