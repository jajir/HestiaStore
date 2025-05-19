package com.coroptis.index.sst;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.log.Log;

public class IndexInternalDefault<K, V> extends SstIndexImpl<K, V> {

    public IndexInternalDefault(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor, final IndexConf conf,
            final Log<K, V> log) {
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
