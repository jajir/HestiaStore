package org.hestiastore.index.segmentindex.core.session;

import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;
import org.slf4j.LoggerFactory;

/**
 * Direct, caller-thread implementation of {@link SegmentIndex}.
 * <p>
 * Executes synchronous API calls on the caller thread while preserving the
 * existing {@link SegmentIndexImpl} retry behavior and iterator invalidation
 * rules.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexInternalConcurrent<K, V> extends AbstractCloseableResource
        implements IndexInternal<K, V> {

    private final SegmentIndexImpl<K, V> delegate;

    /**
     * Creates a concurrent index implementation around an already assembled
     * delegate.
     *
     * @param delegate underlying session implementation
     */
    public IndexInternalConcurrent(final SegmentIndexImpl<K, V> delegate) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
    }

    /**
     * Creates an index and completes startup before returning it.
     *
     * @param directoryFacade      directory facade
     * @param keyTypeDescriptor    key type descriptor
     * @param valueTypeDescriptor  value type descriptor
     * @param conf                 configuration for the index
     * @param executorRegistry     shared executor registry
     * @return ready index
     */
    @SuppressWarnings("java:S107")
    public static <K, V> IndexInternalConcurrent<K, V> createStarted(
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final EffectiveIndexConfiguration<K, V> conf,
            final ExecutorRegistry executorRegistry) {
        final IndexInternalConcurrent<K, V> index = createOpening(
                directoryFacade, keyTypeDescriptor, valueTypeDescriptor, conf,
                executorRegistry);
        index.completeStartup();
        return index;
    }

    /**
     * Creates an index in opening state so the owning bootstrap flow can
     * register cleanup before completing startup.
     *
     * @param directoryFacade      directory facade
     * @param keyTypeDescriptor    key type descriptor
     * @param valueTypeDescriptor  value type descriptor
     * @param conf                 configuration for the index
     * @param executorRegistry     shared executor registry
     * @return index waiting for startup completion
     */
    @SuppressWarnings("java:S107")
    public static <K, V> IndexInternalConcurrent<K, V> createOpening(
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final EffectiveIndexConfiguration<K, V> conf,
            final ExecutorRegistry executorRegistry) {
        return new IndexInternalConcurrent<>(newDelegate(
                directoryFacade, keyTypeDescriptor, valueTypeDescriptor, conf,
                executorRegistry));
    }

    private static <K, V> SegmentIndexImpl<K, V> newDelegate(
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final EffectiveIndexConfiguration<K, V> conf,
            final ExecutorRegistry executorRegistry) {
        return SegmentIndexImpl.open(LoggerFactory.getLogger(
                SegmentIndexImpl.class), directoryFacade, keyTypeDescriptor,
                valueTypeDescriptor, conf, executorRegistry);
    }

    @Override
    public void put(final K key, final V value) {
        delegate.put(key, value);
    }

    @Override
    public V get(final K key) {
        return delegate.get(key);
    }

    @Override
    public void delete(final K key) {
        delegate.delete(key);
    }

    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows) {
        return delegate.getStream(segmentWindows);
    }

    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindow,
            final SegmentIteratorIsolation isolation) {
        return delegate.getStream(segmentWindow, isolation);
    }

    @Override
    public RuntimeTuning runtimeTuning() {
        return delegate.runtimeTuning();
    }

    @Override
    public IndexRuntimeMonitoring runtimeMonitoring() {
        return delegate.runtimeMonitoring();
    }

    @Override
    public SegmentIndexMaintenance maintenance() {
        return delegate.maintenance();
    }

    @Override
    public EntryIterator<K, V> openSegmentIterator(
            final SegmentWindow segmentWindows) {
        return delegate.openSegmentIterator(segmentWindows);
    }

    public EntryIterator<K, V> openSegmentIterator(
            final SegmentWindow segmentWindow,
            final SegmentIteratorIsolation isolation) {
        return delegate.openSegmentIterator(segmentWindow, isolation);
    }

    @Override
    protected void doClose() {
        delegate.close();
    }

    /**
     * Completes startup after the owning bootstrap factory has registered this
     * index for failure cleanup.
     */
    public void completeStartup() {
        delegate.completeStartup();
    }

    /**
     * Marks this not-yet-returned index for failed-startup cleanup.
     *
     * @param failure startup failure
     */
    public void abortStartup(final Throwable failure) {
        delegate.prepareFailedStartupCleanup(Vldtn.requireNonNull(failure,
                "failure"));
    }
}
