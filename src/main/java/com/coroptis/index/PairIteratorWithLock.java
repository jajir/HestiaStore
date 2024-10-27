package com.coroptis.index;

import java.util.NoSuchElementException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PairIteratorWithLock<K, V> implements PairIterator<K, V> {

    private final Logger logger = LoggerFactory
            .getLogger(PairIteratorWithLock.class);
    private final PairIterator<K, V> iterator;
    private final OptimisticLock lock;
    private final String lockedObjectName;

    public PairIteratorWithLock(final PairIterator<K, V> iterator,
            final OptimisticLock optimisticLock,
            final String lockedObjectName) {
        this.iterator = Objects.requireNonNull(iterator,
                "Pair iterator can't be null.");
        this.lock = Objects.requireNonNull(optimisticLock,
                "Optimistic lock can't be null.");
        this.lockedObjectName = Objects.requireNonNull(lockedObjectName,
                "Locked object name can't be null.");
    }

    @Override
    public boolean hasNext() {
        if (lock.isLocked()) {
            logger.debug("Skipping reading data from '{}', it's locked",
                    lockedObjectName);
            return false;
        } else {
            return iterator.hasNext();
        }
    }

    @Override
    public Pair<K, V> next() {
        if (lock.isLocked()) {
            throw new NoSuchElementException(String.format(
                    "Unable to move to next element in iterator '%s' because it's locked.",
                    lockedObjectName));
        }
        return iterator.next();
    }

    @Override
    public void close() {
        iterator.close();
    }

}
