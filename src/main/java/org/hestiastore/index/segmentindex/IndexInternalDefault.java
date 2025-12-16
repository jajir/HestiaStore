package org.hestiastore.index.segmentindex;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;

/**
 * Default single-threaded implementation of {@link IndexInternal}. It inherits
 * the bulk of the segment-index behavior from {@link SegmentIndexImpl} and only
 * exposes an {@link #getStream(SegmentWindow)} implementation that converts
 * the low-level iterator into a Java {@link Stream}.
 *
 * @param <K> key type handled by the index
 * @param <V> value type handled by the index
 */
public class IndexInternalDefault<K, V> extends SegmentIndexImpl<K, V> {

    public IndexInternalDefault(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf) {
        super(directory, keyTypeDescriptor, valueTypeDescriptor, conf);
    }

    /**
     * Streams over entries contained in the given segment window. The returned
     * stream is non-parallel and closes the underlying iterator when the stream
     * is closed by the caller.
     */
    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindow) {
        indexState.tryPerformOperation();
        final EntryIterator<K, V> iterator = openSegmentIterator(segmentWindow);
        final EntryIteratorToSpliterator<K, V> spliterator = new EntryIteratorToSpliterator<K, V>(
                iterator, keyTypeDescriptor);
        return StreamSupport.stream(spliterator, false).onClose(() -> {
            iterator.close();
        });
    }
}
