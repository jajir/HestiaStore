package org.hestiastore.index.sst;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.PairReader;
import org.hestiastore.index.Vldtn;

/**
 * Convert PairIterator to PairReader.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class PairReaderFromIterator<K, V> implements PairReader<K, V> {

    private final PairIterator<K, V> pairIterator;

    PairReaderFromIterator(final PairIterator<K, V> pairIterator) {
        this.pairIterator = Vldtn.requireNonNull(pairIterator, "pairIterator");
    }

    @Override
    public Pair<K, V> read() {
        if (pairIterator.hasNext()) {
            return pairIterator.next();
        } else {
            return null;
        }
    }

}
