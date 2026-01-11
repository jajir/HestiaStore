package org.hestiastore.index.segmentindex;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.async.AsyncDirectory;

/**
 * Direct, caller-thread implementation of {@link SegmentIndex}.
 * <p>
 * Executes synchronous API calls on the caller thread while preserving the
 * existing SegmentIndexImpl retry behavior and iterator invalidation rules.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class IndexInternalConcurrent<K, V> extends SegmentIndexImpl<K, V> {

    public IndexInternalConcurrent(final AsyncDirectory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf) {
        super(directoryFacade, keyTypeDescriptor, valueTypeDescriptor, conf);
    }

    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindow) {
        getIndexState().tryPerformOperation();
        final EntryIterator<K, V> iterator = openSegmentIterator(
                segmentWindow);
        try {
            final List<Entry<K, V>> snapshot = new ArrayList<>();
            while (iterator.hasNext()) {
                snapshot.add(iterator.next());
            }
            return snapshot.stream();
        } finally {
            iterator.close();
        }
    }

    @Override
    public void flush() {
        invalidateSegmentIterators();
        super.flush();
    }

    @Override
    public void flushAndWait() {
        invalidateSegmentIterators();
        super.flushAndWait();
    }

    @Override
    public void compact() {
        invalidateSegmentIterators();
        super.compact();
    }

    @Override
    public void compactAndWait() {
        invalidateSegmentIterators();
        super.compactAndWait();
    }

    @Override
    public void checkAndRepairConsistency() {
        invalidateSegmentIterators();
        super.checkAndRepairConsistency();
    }
}
