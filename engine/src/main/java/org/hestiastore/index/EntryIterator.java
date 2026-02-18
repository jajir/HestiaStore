package org.hestiastore.index;

import java.util.Iterator;

/**
 * Define key value entry iterator. It allows to go through all records and
 * further works with them. When the iterator is created the first call to
 * {@link #next()} returns the initial entry.
 *
 * Must be closed to release resources.
 *
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public interface EntryIterator<K, V>
        extends Iterator<Entry<K, V>>, CloseableResource {

    /**
     * Create entry iterator from standard iterator. Created iterator doesn't
     * need to be closed.
     * 
     * @param <M>      key type
     * @param <N>      value type
     * @param iterator required iterator
     * @return entry iterator
     */
    public static <M, N> EntryIterator<M, N> make(
            final Iterator<Entry<M, N>> iterator) {
        Vldtn.requireNonNull(iterator, "iterator");
        class Adapter extends AbstractCloseableResource
                implements EntryIterator<M, N> {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Entry<M, N> next() {
                return iterator.next();
            }

            @Override
            protected void doClose() {
                // nothing to close
            }
        }
        return new Adapter();
    }

}
