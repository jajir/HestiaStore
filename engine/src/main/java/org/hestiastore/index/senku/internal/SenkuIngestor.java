package org.hestiastore.index.senku.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.senku.SenkuMergeFunction;

/**
 * Owns the single locked ingestion map and synchronous flush trigger.
 */
final class SenkuIngestor<K, V> {

    private final ReentrantLock lock;
    private final Condition ingestionMayProceed;
    private final SenkuMergeFunction<K, V> mergeFunction;
    private final SenkuFlushWriter<K, V> flushWriter;
    private final int maxInMemoryEntries;
    private final Map<K, V> entries;

    private long nextFlushId;
    private boolean flushIdsExhausted;
    private boolean accepting = true;
    private boolean paused;

    SenkuIngestor(final ReentrantLock lock,
            final SenkuMergeFunction<K, V> mergeFunction,
            final SenkuFlushWriter<K, V> flushWriter,
            final int maxInMemoryEntries, final int initialMapCapacity) {
        this.lock = Vldtn.requireNonNull(lock, "lock");
        ingestionMayProceed = lock.newCondition();
        this.mergeFunction = Vldtn.requireNonNull(mergeFunction,
                "mergeFunction");
        this.flushWriter = Vldtn.requireNonNull(flushWriter, "flushWriter");
        this.maxInMemoryEntries = Vldtn.requireGreaterThanZero(
                maxInMemoryEntries, "maxInMemoryEntries");
        this.entries = new HashMap<>(Vldtn.requireGreaterThanZero(
                initialMapCapacity, "initialMapCapacity"));
    }

    void put(final K key, final V value) {
        lock.lock();
        try {
            while (paused && accepting) {
                ingestionMayProceed.awaitUninterruptibly();
            }
            if (!accepting) {
                throw new IndexException("Senku index no longer accepts writes.");
            }
            final K validatedKey = Vldtn.requireNonNull(key, "key");
            final V validatedValue = Vldtn.requireNonNull(value, "value");
            final V current = entries.get(validatedKey);
            final V stored = current == null ? validatedValue
                    : merge(validatedKey, current, validatedValue);
            entries.put(validatedKey, stored);
            if (entries.size() == maxInMemoryEntries) {
                flushLocked();
            }
        } finally {
            lock.unlock();
        }
    }

    boolean flushRemaining() {
        lock.lock();
        try {
            if (entries.isEmpty()) {
                return false;
            }
            flushLocked();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Changes queue-driven ingestion backpressure without directory I/O.
     *
     * @param value true to pause new puts; false to resume them
     */
    void setPaused(final boolean value) {
        lock.lock();
        try {
            final boolean previous = paused;
            paused = accepting && value;
            if (previous && !paused) {
                ingestionMayProceed.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Closes admission, wakes waiters, and flushes the final partial map.
     *
     * @return true when a final flush was published
     */
    boolean stopAcceptingAndFlush() {
        lock.lock();
        try {
            if (!accepting) {
                throw new IndexException("Senku index no longer accepts writes.");
            }
            accepting = false;
            paused = false;
            ingestionMayProceed.signalAll();
            if (entries.isEmpty()) {
                return false;
            }
            flushLocked();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Stops admission and wakes blocked callers after runtime failure.
     */
    void fail() {
        lock.lock();
        try {
            accepting = false;
            paused = false;
            ingestionMayProceed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    boolean isPaused() {
        lock.lock();
        try {
            return paused;
        } finally {
            lock.unlock();
        }
    }

    int size() {
        lock.lock();
        try {
            return entries.size();
        } finally {
            lock.unlock();
        }
    }

    private V merge(final K key, final V first, final V second) {
        try {
            return Vldtn.requireNonNull(mergeFunction.apply(key, first, second),
                    "mergedValue");
        } catch (Exception e) {
            if (e instanceof IndexException) {
                throw (IndexException) e;
            }
            throw new IndexException("Senku merge function failed.", e);
        }
    }

    private void flushLocked() {
        if (flushIdsExhausted) {
            throw new IndexException("Senku flush ID sequence is exhausted.");
        }
        flushWriter.write(nextFlushId, entries);
        entries.clear();
        if (nextFlushId == Long.MAX_VALUE) {
            flushIdsExhausted = true;
        } else {
            nextFlushId++;
        }
    }
}
