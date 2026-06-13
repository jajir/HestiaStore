package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;

/**
 * Default data-path access used after session lifecycle checks have passed.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexDataAccessImpl<K, V>
        implements SegmentIndexDataAccess<K, V> {

    private final SegmentIndexOperationAccess<K, V> operationAccess;
    private final SegmentTopologyRuntimeAccess<K, V> topologyRuntime;

    /**
     * Creates data access backed by operation and topology collaborators.
     *
     * @param operationAccess point-operation access
     * @param topologyRuntime segment topology and iterator access
     */
    SegmentIndexDataAccessImpl(
            final SegmentIndexOperationAccess<K, V> operationAccess,
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime) {
        this.operationAccess = Vldtn.requireNonNull(operationAccess,
                "operationAccess");
        this.topologyRuntime = Vldtn.requireNonNull(topologyRuntime,
                "topologyRuntime");
    }

    /** {@inheritDoc} */
    @Override
    public void put(final K key, final V value) {
        operationAccess.put(key, value);
    }

    /** {@inheritDoc} */
    @Override
    public V get(final K key) {
        return operationAccess.get(key);
    }

    /** {@inheritDoc} */
    @Override
    public void delete(final K key) {
        operationAccess.delete(key);
    }

    /** {@inheritDoc} */
    @Override
    public EntryIterator<K, V> openSegmentIterator(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        return topologyRuntime.openSegmentIterator(segmentId, isolation);
    }

    /** {@inheritDoc} */
    @Override
    public EntryIterator<K, V> openWindowIterator(
            final SegmentWindow segmentWindow,
            final SegmentIteratorIsolation isolation) {
        return topologyRuntime.openWindowIterator(segmentWindow, isolation);
    }
}
