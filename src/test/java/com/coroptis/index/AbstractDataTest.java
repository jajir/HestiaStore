package com.coroptis.index;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public abstract class AbstractDataTest {

    /**
     * Convert pair iterator data to list
     * 
     * @param <M>      key type
     * @param <N>      value type
     * @param iterator
     * @returnlist of pairs with data from list
     */
    protected <M, N> List<Pair<M, N>> toList(
            final Stream<Pair<M, N>> iterator) {
        final ArrayList<Pair<M, N>> out = new ArrayList<>();
        iterator.forEach(pair -> out.add(pair));
        iterator.close();
        return out;
    }

    /**
     * Convert pair iterator data to list
     * 
     * @param <M>      key type
     * @param <N>      value type
     * @param iterator
     * @returnlist of pairs with data from list
     */
    protected <M, N> List<Pair<M, N>> toList(
            final PairIterator<M, N> iterator) {
        final ArrayList<Pair<M, N>> out = new ArrayList<>();
        while (iterator.hasNext()) {
            out.add(iterator.next());
        }
        iterator.close();
        return out;
    }

}
