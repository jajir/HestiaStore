package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Abstract class for data tests
 * 
 * Don't extend it, use static imports.
 * 
 */
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
     * Compare two key value pairs.
     * 
     * @param expectedPair expected pair
     * @param pair         verified pair
     */
    protected void verifyEquals(final Pair<String, Integer> expectedPair,
            final Pair<String, Integer> pair) {
        assertNotNull(expectedPair);
        assertNotNull(pair);
        assertEquals(expectedPair.getKey(), pair.getKey());
        assertEquals(expectedPair.getValue(), pair.getValue());
    }

    /**
     * Convert pair iterator data to list
     * 
     * @param <M>      key type
     * @param <N>      value type
     * @param iterator
     * @returnlist of pairs with data from list
     */
    protected static <M, N> List<Pair<M, N>> toList(
            final PairIterator<M, N> iterator) {
        final ArrayList<Pair<M, N>> out = new ArrayList<>();
        while (iterator.hasNext()) {
            out.add(iterator.next());
        }
        iterator.close();
        return out;
    }

    /**
     * Verify that data from iterator are same as expecetd values
     * 
     * @param <M>          key type
     * @param <N>          value type
     * @param pairs        required list of expected data in segment
     * @param pairIterator required pair iterator
     */
    public static <M, N> void verifyIteratorData(final List<Pair<M, N>> pairs,
            final PairIterator<M, N> pairIterator) {
        final List<Pair<M, N>> data = toList(pairIterator);
        assertEquals(pairs.size(), data.size(),
                "Unexpected iterator data size");
        for (int i = 0; i < pairs.size(); i++) {
            final Pair<M, N> expectedPair = pairs.get(i);
            final Pair<M, N> realPair = data.get(i);
            assertEquals(expectedPair, realPair);
        }
    }

}
