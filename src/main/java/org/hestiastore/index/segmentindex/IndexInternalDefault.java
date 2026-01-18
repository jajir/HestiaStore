package org.hestiastore.index.segmentindex;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.segment.SegmentIteratorIsolation;

/**
 * Default single-threaded implementation of {@link IndexInternal}. It inherits
 * the bulk of the segment-index behavior from {@link SegmentIndexImpl} and only
 * exposes {@link #getStream(SegmentWindow, SegmentIteratorIsolation)}
 * implementations that convert low-level iterators into Java {@link Stream}s.
 *
 * @param <K> key type handled by the index
 * @param <V> value type handled by the index
 */
public class IndexInternalDefault<K, V> extends SegmentIndexImpl<K, V> {

    public IndexInternalDefault(final AsyncDirectory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf) {
        super(directoryFacade, keyTypeDescriptor, valueTypeDescriptor, conf);
    }

    /**
     * Streams over entries contained in the given segment window. The returned
     * stream is non-parallel and closes the underlying iterator when the stream
     * is closed by the caller.
     */
    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindow) {
        return getStream(segmentWindow, SegmentIteratorIsolation.FAIL_FAST);
    }

    /**
     * Streams over entries with the requested iterator isolation level. The
     * returned stream is non-parallel and closes the underlying iterator when
     * the stream is closed by the caller.
     */
    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindow,
            final SegmentIteratorIsolation isolation) {
        getIndexState().tryPerformOperation();
        final EntryIterator<K, V> iterator = openSegmentIterator(segmentWindow,
                isolation);
        final EntryIteratorToSpliterator<K, V> spliterator = new EntryIteratorToSpliterator<K, V>(
                iterator, keyTypeDescriptor);
        return StreamSupport.stream(spliterator, false)
                .onClose(iterator::close);
    }
}
