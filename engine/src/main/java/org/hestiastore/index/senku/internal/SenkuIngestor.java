package org.hestiastore.index.senku.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.senku.SenkuMergeFunction;

/**
 * Owns the locked active ingestion map and rotates full maps for flushing
 * outside the ingestion lock.
 */
final class SenkuIngestor<K, V> {

    private final ReentrantLock lock;
    private final Condition ingestionMayProceed;
    private final SenkuMergeFunction<K, V> mergeFunction;
    private final SenkuFlushWriter<K, V> flushWriter;
    private final int maxInMemoryEntries;
    private final int initialMapCapacity;

    private Map<K, V> entries;
    private Map<K, V> flushingEntries;
    private long flushingId;
    private long nextFlushId;
    private boolean flushIdsExhausted;
    private boolean flushing;
    private boolean accepting = true;
    private boolean paused;
    private IndexException flushFailure;

    /**
     * Creates an ingestor backed by one active map and at most one map being
     * flushed.
     *
     * @param lock               shared writing lifecycle lock
     * @param mergeFunction      duplicate-key merge function
     * @param flushWriter        persistent flush writer
     * @param maxInMemoryEntries maximum distinct keys per map
     * @param initialMapCapacity initial capacity of each rotated map
     */
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
        this.initialMapCapacity = Vldtn.requireGreaterThanZero(
                initialMapCapacity, "initialMapCapacity");
        this.entries = new HashMap<>(this.initialMapCapacity);
    }

    /**
     * Adds or merges an entry and writes a claimed full map without holding the
     * ingestion lock.
     *
     * @param key   non-null key
     * @param value non-null value
     */
    void put(final K key, final V value) {
        final boolean flush;
        lock.lock();
        try {
            flush = putLocked(key, value);
        } finally {
            lock.unlock();
        }
        if (flush) {
            flushClaimed();
        }
    }

    /**
     * Adds or merges an entry while the caller holds the ingestion lock.
     *
     * @param key   non-null key
     * @param value non-null value
     * @return true when the caller claimed responsibility for flushing a full
     *         rotated map
     */
    boolean putLocked(final K key, final V value) {
        while ((paused || flushing && entries.size() == maxInMemoryEntries)
                && accepting) {
            ingestionMayProceed.awaitUninterruptibly();
        }
        requireAcceptingLocked();
        final K validatedKey = Vldtn.requireNonNull(key, "key");
        final V validatedValue = Vldtn.requireNonNull(value, "value");
        final V current = entries.get(validatedKey);
        final V stored = current == null ? validatedValue
                : merge(validatedKey, current, validatedValue);
        entries.put(validatedKey, stored);
        if (entries.size() == maxInMemoryEntries && !flushing) {
            claimFlushLocked();
            return true;
        }
        return false;
    }

    /**
     * Flushes a previously claimed map and any next map filled while that I/O
     * was running.
     */
    void flushClaimed() {
        while (true) {
            final Map<K, V> batch;
            final long generation;
            lock.lock();
            try {
                batch = flushingEntries;
                generation = flushingId;
            } finally {
                lock.unlock();
            }
            try {
                flushWriter.write(generation, batch);
            } catch (IndexException e) {
                recordFlushFailure(e);
                throw e;
            }

            lock.lock();
            try {
                advanceFlushIdLocked();
                if (accepting && entries.size() == maxInMemoryEntries) {
                    try {
                        claimNextFlushLocked();
                    } catch (IndexException e) {
                        recordFlushFailureLocked(e);
                        throw e;
                    }
                    ingestionMayProceed.signalAll();
                } else {
                    finishFlushLocked();
                    return;
                }
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Flushes the current partial map after any in-progress flush completes.
     *
     * @return true when a map was published
     */
    boolean flushRemaining() {
        lock.lock();
        try {
            awaitFlushLocked();
            if (entries.isEmpty()) {
                return false;
            }
            claimFlushLocked();
        } finally {
            lock.unlock();
        }
        flushClaimed();
        return true;
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
            requireAcceptingLocked();
            accepting = false;
            paused = false;
            ingestionMayProceed.signalAll();
            awaitFlushLocked();
            if (entries.isEmpty()) {
                return false;
            }
            claimFlushLocked();
        } finally {
            lock.unlock();
        }
        flushClaimed();
        return true;
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

    private void awaitFlushLocked() {
        while (flushing) {
            ingestionMayProceed.awaitUninterruptibly();
        }
        if (flushFailure != null) {
            throw flushFailure;
        }
    }

    private void requireAcceptingLocked() {
        if (flushFailure != null) {
            throw flushFailure;
        }
        if (!accepting) {
            throw new IndexException("Senku index no longer accepts writes.");
        }
    }

    private void claimFlushLocked() {
        claimNextFlushLocked();
        flushing = true;
    }

    private void claimNextFlushLocked() {
        if (flushIdsExhausted) {
            throw new IndexException("Senku flush ID sequence is exhausted.");
        }
        flushingEntries = entries;
        flushingId = nextFlushId;
        entries = new HashMap<>(initialMapCapacity);
    }

    private void advanceFlushIdLocked() {
        if (nextFlushId == Long.MAX_VALUE) {
            flushIdsExhausted = true;
        } else {
            nextFlushId++;
        }
    }

    private void finishFlushLocked() {
        flushing = false;
        flushingEntries = null;
        ingestionMayProceed.signalAll();
    }

    private void recordFlushFailure(final IndexException failure) {
        lock.lock();
        try {
            recordFlushFailureLocked(failure);
        } finally {
            lock.unlock();
        }
    }

    private void recordFlushFailureLocked(final IndexException failure) {
        flushFailure = failure;
        accepting = false;
        paused = false;
        finishFlushLocked();
    }
}
