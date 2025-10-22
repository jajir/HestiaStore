package org.hestiastore.index;

import java.util.Iterator;

/**
 * Define key value pair iterator. It allows to go through all records and
 * further works with them. When object is initialized method
 * {@link #getNextPair()} return null.
 * 
 * Must be closed to release resources.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public interface PairIterator<K, V>
        extends Iterator<Pair<K, V>>, CloseableResource {

    /**
     * Create pair iterator from standard iterator. Created iterator doesn't
     * need to be closed.
     * 
     * @param <M>      key type
     * @param <N>      value type
     * @param iterator required iterator
     * @return pair iterator
     */
    public static <M, N> PairIterator<M, N> make(
            final Iterator<Pair<M, N>> iterator) {
        Vldtn.requireNonNull(iterator, "iterator");
        class Adapter extends AbstractCloseableResource
                implements PairIterator<M, N> {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Pair<M, N> next() {
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
