package org.hestiastore.index;

import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Wraps a {@link PairIterator} to provide a {@link Stream} interface.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class PairIteratorStreamer<K, V> extends AbstractCloseableResource {

    private final PairIterator<K, V> iterator;

    /**
     * Constructs a new {@link PairIteratorStreamer} with the given iterator.
     *
     * @param iterator the iterator to wrap
     */
    public PairIteratorStreamer(final PairIterator<K, V> iterator) {
        this.iterator = iterator;
    }

    /**
     * Returns a {@link Stream} of pairs from the underlying iterator.
     *
     * @return a stream of pairs
     */
    public Stream<Pair<K, V>> stream() {
        if (iterator == null) {
            return Stream.empty();
        }
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    @Override
    protected void doClose() {
        if (iterator != null) {
            iterator.close();
        }
    }

}
