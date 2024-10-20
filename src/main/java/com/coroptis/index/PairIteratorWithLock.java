package com.coroptis.index;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PairIteratorWithLock<K, V> implements PairIterator<K, V> {

    private final Logger logger = LoggerFactory.getLogger(PairIteratorWithLock.class);
    private final PairIterator<K, V> iterator;
    private final OptimisticLock lock;

    public PairIteratorWithLock(final PairIterator<K, V> iterator,
            final OptimisticLock optimisticLock) {
        this.iterator = Objects.requireNonNull(iterator,
                "Pair iterator can't be null.");
        this.lock = Objects.requireNonNull(optimisticLock,
                "Optimistic lock can't be null.");
    }

    @Override
    public boolean hasNext() {
        if (lock.isLocked()) {
            logger.debug("Skipping reading data from segment, it's locked");
            return false;
        } else {
            return iterator.hasNext();
        }
    }

    @Override
    public Pair<K, V> next() {
        return iterator.next();
    }

    @Override
    public void close() {
        iterator.close();
    }

}
