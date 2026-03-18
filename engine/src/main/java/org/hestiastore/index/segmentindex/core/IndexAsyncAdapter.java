package org.hestiastore.index.segmentindex.core;

import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.SegmentWindow;

/**
 * Thin facade that preserves the async-oriented API surface while delegating
 * execution to the wrapped index.
 *
 * @param <K> type of keys stored in the index
 * @param <V> type of values stored in the index
 */
class IndexAsyncAdapter<K, V> extends AbstractCloseableResource
        implements SegmentIndex<K, V> {

    private final SegmentIndex<K, V> index;

    IndexAsyncAdapter(final SegmentIndex<K, V> index) {
        this.index = Vldtn.requireNonNull(index, "index");
    }

    /** {@inheritDoc} */
    @Override
    public void put(final K key, final V value) {
        index.put(key, value);
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<Void> putAsync(final K key, final V value) {
        return index.putAsync(key, value);
    }

    /** {@inheritDoc} */
    @Override
    public V get(final K key) {
        return index.get(key);
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<V> getAsync(final K key) {
        return index.getAsync(key);
    }

    /** {@inheritDoc} */
    @Override
    public void delete(final K key) {
        index.delete(key);
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<Void> deleteAsync(final K key) {
        return index.deleteAsync(key);
    }

    /** {@inheritDoc} */
    @Override
    public void compact() {
        index.compact();
    }

    /** {@inheritDoc} */
    @Override
    public void compactAndWait() {
        index.compactAndWait();
    }

    /** {@inheritDoc} */
    @Override
    public void flush() {
        index.flush();
    }

    /** {@inheritDoc} */
    @Override
    public void flushAndWait() {
        index.flushAndWait();
    }

    /** {@inheritDoc} */
    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows) {
        return index.getStream(segmentWindows);
    }

    /** {@inheritDoc} */
    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows,
            final SegmentIteratorIsolation isolation) {
        return index.getStream(segmentWindows, isolation);
    }

    /** {@inheritDoc} */
    @Override
    public void checkAndRepairConsistency() {
        index.checkAndRepairConsistency();
    }

    /** {@inheritDoc} */
    @Override
    public IndexConfiguration<K, V> getConfiguration() {
        return index.getConfiguration();
    }

    /** {@inheritDoc} */
    @Override
    public SegmentIndexState getState() {
        return index.getState();
    }

    /** {@inheritDoc} */
    @Override
    public SegmentIndexMetricsSnapshot metricsSnapshot() {
        return index.metricsSnapshot();
    }

    /** {@inheritDoc} */
    @Override
    public IndexControlPlane controlPlane() {
        return index.controlPlane();
    }

    /** {@inheritDoc} */
    @Override
    protected void doClose() {
        index.close();
    }
}
