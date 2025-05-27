package org.hestiastore.index.sst;

import java.util.Objects;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.PairReader;

/**
 * Convert PairIterator to PairReader.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class PairReaderFromIterator<K, V> implements PairReader<K, V> {

    private final PairIterator<K, V> pairIterator;

    PairReaderFromIterator(final PairIterator<K, V> pairIterator) {
        this.pairIterator = Objects.requireNonNull(pairIterator,
                "Pair iterator cannot be null");
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
