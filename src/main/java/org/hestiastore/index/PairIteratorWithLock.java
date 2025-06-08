package org.hestiastore.index;

import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PairIteratorWithLock<K, V> implements PairIterator<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final PairIterator<K, V> iterator;
    private final OptimisticLock lock;
    private final String lockedObjectName;

    public PairIteratorWithLock(final PairIterator<K, V> iterator,
            final OptimisticLock optimisticLock,
            final String lockedObjectName) {
        this.iterator = Vldtn.requireNonNull(iterator, "iterator");
        this.lock = Vldtn.requireNonNull(optimisticLock, "optimisticLock");
        this.lockedObjectName = Vldtn.requireNonNull(lockedObjectName,
                "lockedObjectName");
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
