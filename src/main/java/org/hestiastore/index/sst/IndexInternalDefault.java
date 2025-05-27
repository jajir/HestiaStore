package org.hestiastore.index.sst;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.log.Log;

public class IndexInternalDefault<K, V> extends SstIndexImpl<K, V> {

    public IndexInternalDefault(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf, final Log<K, V> log) {
        super(directory, keyTypeDescriptor, valueTypeDescriptor, conf, log);
    }

    @Override
    public Stream<Pair<K, V>> getStream(final SegmentWindow segmentWindow) {
        indexState.tryPerformOperation();
        final PairIterator<K, V> iterator = openSegmentIterator(segmentWindow);
        final PairIteratorToSpliterator<K, V> spliterator = new PairIteratorToSpliterator<K, V>(
                iterator, keyTypeDescriptor);
        return StreamSupport.stream(spliterator, false).onClose(() -> {
            iterator.close();
        });
    }
}
