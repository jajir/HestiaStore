package org.hestiastore.index;

import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntryIteratorWithLock<K, V> extends AbstractCloseableResource
        implements EntryIterator<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final EntryIterator<K, V> iterator;
    private final OptimisticLock lock;
    private final String lockedObjectName;

    public EntryIteratorWithLock(final EntryIterator<K, V> iterator,
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
            // Concurrent writes invalidate the iterator. End iteration
            // gracefully instead of failing, so callers can re-open if needed.
            logger.debug(
                    "Iteration for '{}' interrupted by a concurrent write.",
                    lockedObjectName);
            return false;
        }
        return iterator.hasNext();
    }

    @Override
    public Entry<K, V> next() {
        if (lock.isLocked()) {
            throw new NoSuchElementException(String.format(
                    "Iterator for '%s' was invalidated by a concurrent write.",
                    lockedObjectName));
        }
        return iterator.next();
    }

    @Override
    protected void doClose() {
        iterator.close();
    }

}
