package org.hestiastore.index;

import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Wraps a {@link EntryIterator} to provide a {@link Stream} interface.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class EntryIteratorStreamer<K, V> extends AbstractCloseableResource {

    private final EntryIterator<K, V> iterator;

    /**
     * Constructs a new {@link EntryIteratorStreamer} with the given iterator.
     *
     * @param iterator the iterator to wrap
     */
    public EntryIteratorStreamer(final EntryIterator<K, V> iterator) {
        this.iterator = iterator;
    }

    /**
     * Returns a {@link Stream} of entries from the underlying iterator.
     *
     * @return a stream of entries
     */
    public Stream<Entry<K, V>> stream() {
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
