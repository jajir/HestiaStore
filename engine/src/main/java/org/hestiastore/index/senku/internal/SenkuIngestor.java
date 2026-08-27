package org.hestiastore.index.senku.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.senku.SenkuMergeFunction;

/**
 * Owns striped active ingestion maps and rotates full batches for flushing
 * outside the mutation locks. Each stripe publishes its size at intervals;
 * the published total can lag the actual distinct-key count by less than 25
 * percent of the threshold without a shared increment on every put.
 */
final class SenkuIngestor<K, V> {

    private static final int INGESTION_STRIPE_COUNT = 32;

    private final ReentrantLock controlLock;
    private final ReentrantLock[] mutationLocks =
            new ReentrantLock[INGESTION_STRIPE_COUNT];
    private final Condition ingestionMayProceed;
    private final SenkuMergeFunction<K, V> mergeFunction;
    private final SenkuFlushWriter<K, V> flushWriter;
    private final int maxInMemoryEntries;
    private final int initialMapCapacity;
    private final int countCheckInterval;

    private volatile List<Map<K, V>> entries;
    private volatile int[] stripeEntryCounts;
    private volatile AtomicIntegerArray publishedStripeSizes;
    private volatile int[] nextCountChecks;
    private List<Map<K, V>> flushingEntries;
    private long flushingId;
    private long nextFlushId;
    private boolean flushIdsExhausted;
    private boolean flushing;
    private volatile boolean accepting = true;
    private volatile boolean paused;
    private volatile boolean rotationRequested;
    private volatile IndexException flushFailure;

    /**
     * Creates an ingestor backed by one active striped batch and at most one
     * batch being flushed. Independent mutation stripes allow parallel puts;
     * taking every stripe makes batch rotation safe.
     *
     * @param controlLock        lifecycle, rotation, and backpressure lock
     * @param mergeFunction      duplicate-key merge function
     * @param flushWriter        persistent flush writer
     * @param maxInMemoryEntries approximate distinct-key rotation threshold
     * @param initialMapCapacity total initial capacity of each rotated batch
     */
    SenkuIngestor(final ReentrantLock controlLock,
            final SenkuMergeFunction<K, V> mergeFunction,
            final SenkuFlushWriter<K, V> flushWriter,
            final int maxInMemoryEntries, final int initialMapCapacity) {
        this.controlLock = Vldtn.requireNonNull(controlLock, "controlLock");
        ingestionMayProceed = controlLock.newCondition();
        this.mergeFunction = Vldtn.requireNonNull(mergeFunction,
                "mergeFunction");
        this.flushWriter = Vldtn.requireNonNull(flushWriter, "flushWriter");
        this.maxInMemoryEntries = Vldtn.requireGreaterThanZero(
                maxInMemoryEntries, "maxInMemoryEntries");
        this.initialMapCapacity = Vldtn.requireGreaterThanZero(
                initialMapCapacity, "initialMapCapacity");
        countCheckInterval = Math.max(1,
                maxInMemoryEntries / (INGESTION_STRIPE_COUNT * 4));
        entries = newMaps();
        resetCountSampling();
        for (int index = 0; index < mutationLocks.length; index++) {
            mutationLocks[index] = new ReentrantLock();
        }
    }

    /**
     * Atomically adds or merges an entry. Distinct keys on separate mutation
     * stripes may proceed in parallel; one key is always merged serially.
     *
     * @param key   non-null key
     * @param value non-null value
     */
    void put(final K key, final V value) {
        final K validatedKey = Vldtn.requireNonNull(key, "key");
        final V validatedValue = Vldtn.requireNonNull(value, "value");
        final int stripe = stripe(validatedKey);
        final ReentrantLock mutationLock = mutationLocks[stripe];
        boolean accepted = false;
        boolean rotate = false;
        while (!accepted) {
            if (paused || rotationRequested) {
                awaitAdmission();
            }
            mutationLock.lock();
            try {
                requireAccepting();
                if (!paused && !rotationRequested) {
                    final Map<K, V> stripeEntries = entries.get(stripe);
                    final V current = stripeEntries.get(validatedKey);
                    if (current == null) {
                        stripeEntries.put(validatedKey, validatedValue);
                        stripeEntryCounts[stripe]++;
                        sampleCount(stripe, stripeEntryCounts[stripe]);
                    } else {
                        stripeEntries.put(validatedKey, merge(validatedKey,
                                current, validatedValue));
                    }
                    rotate = rotationRequested;
                    accepted = true;
                }
            } finally {
                mutationLock.unlock();
            }
        }
        if (rotate) {
            rotateAndFlush();
        }
    }

    /**
     * Flushes the current partial map after any in-progress flush completes.
     *
     * @return true when a map was published
     */
    boolean flushRemaining() {
        controlLock.lock();
        try {
            awaitFlushLocked();
            rotationRequested = true;
            lockAllMutations();
            try {
                if (entryCountLocked() == 0) {
                    cancelRotationLocked();
                    return false;
                }
                claimFlushLocked();
            } finally {
                unlockAllMutations();
            }
        } finally {
            controlLock.unlock();
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
        controlLock.lock();
        try {
            final boolean previous = paused;
            paused = accepting && value;
            if (previous && !paused) {
                ingestionMayProceed.signalAll();
            }
        } finally {
            controlLock.unlock();
        }
    }

    /**
     * Closes admission, waits for any detached-map flush, and flushes the final
     * active map.
     *
     * @return true when a final flush was published
     */
    boolean stopAcceptingAndFlush() {
        controlLock.lock();
        try {
            requireAccepting();
            accepting = false;
            paused = false;
            ingestionMayProceed.signalAll();
            awaitFlushLocked();
            lockAllMutations();
            try {
                if (entryCountLocked() == 0) {
                    return false;
                }
                claimFlushLocked();
            } finally {
                unlockAllMutations();
            }
        } finally {
            controlLock.unlock();
        }
        flushClaimed();
        return true;
    }

    /**
     * Stops admission and wakes blocked callers after runtime failure.
     */
    void fail() {
        controlLock.lock();
        try {
            accepting = false;
            paused = false;
            ingestionMayProceed.signalAll();
        } finally {
            controlLock.unlock();
        }
    }

    /**
     * Returns whether queue-driven ingestion is currently paused.
     *
     * @return true when new puts wait for a resume signal
     */
    boolean isPaused() {
        return paused;
    }

    /**
     * Returns the distinct-key count in the active striped batch.
     *
     * @return active batch size
     */
    int size() {
        lockAllMutations();
        try {
            return entryCountLocked();
        } finally {
            unlockAllMutations();
        }
    }

    private void awaitAdmission() {
        controlLock.lock();
        try {
            while ((paused || rotationRequested) && accepting) {
                ingestionMayProceed.awaitUninterruptibly();
            }
            requireAccepting();
        } finally {
            controlLock.unlock();
        }
    }

    private void rotateAndFlush() {
        boolean claimed = false;
        controlLock.lock();
        try {
            if (accepting && rotationRequested && !flushing) {
                lockAllMutations();
                try {
                    if (accepting && rotationRequested && !flushing) {
                        claimFlushLocked();
                        claimed = true;
                    }
                } finally {
                    unlockAllMutations();
                }
            }
        } finally {
            controlLock.unlock();
        }
        if (claimed) {
            flushClaimed();
        }
    }

    private void flushClaimed() {
        while (true) {
            final List<Map<K, V>> batchMaps;
            final long generation;
            controlLock.lock();
            try {
                batchMaps = flushingEntries;
                generation = flushingId;
            } finally {
                controlLock.unlock();
            }
            try {
                flushWriter.write(generation, batchMaps);
            } catch (IndexException e) {
                recordFlushFailure(e);
                throw e;
            }

            controlLock.lock();
            try {
                advanceFlushIdLocked();
                if (accepting && rotationRequested) {
                    lockAllMutations();
                    try {
                        claimNextFlushLocked();
                    } catch (IndexException e) {
                        recordFlushFailureLocked(e);
                        throw e;
                    } finally {
                        unlockAllMutations();
                    }
                } else {
                    finishFlushLocked();
                    return;
                }
            } finally {
                controlLock.unlock();
            }
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

    private void requireAccepting() {
        final IndexException failure = flushFailure;
        if (failure != null) {
            throw failure;
        }
        if (!accepting) {
            throw new IndexException("Senku index no longer accepts writes.");
        }
    }

    private int stripe(final K key) {
        return stripeForHash(key.hashCode());
    }

    /**
     * Applies the MurmurHash3 32-bit avalanche finalizer before selecting a
     * mutation stripe. This avoids conditioning each stripe on the same low
     * spread bits used by the stripe's {@link HashMap} bucket index.
     *
     * @param hashCode key hash code
     * @return mutation stripe from zero through 31
     */
    static int stripeForHash(final int hashCode) {
        int hash = hashCode;
        hash ^= hash >>> 16;
        hash *= 0x85ebca6b;
        hash ^= hash >>> 13;
        hash *= 0xc2b2ae35;
        hash ^= hash >>> 16;
        return hash & (INGESTION_STRIPE_COUNT - 1);
    }

    private void lockAllMutations() {
        for (ReentrantLock mutationLock : mutationLocks) {
            mutationLock.lock();
        }
    }

    private void unlockAllMutations() {
        for (int index = mutationLocks.length - 1; index >= 0; index--) {
            mutationLocks[index].unlock();
        }
    }

    private void cancelRotationLocked() {
        rotationRequested = false;
        ingestionMayProceed.signalAll();
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
        entries = newMaps();
        resetCountSampling();
        cancelRotationLocked();
    }

    private void sampleCount(final int stripe, final int stripeSize) {
        if (stripeSize < nextCountChecks[stripe]) {
            return;
        }
        publishedStripeSizes.set(stripe, stripeSize);
        nextCountChecks[stripe] = stripeSize + countCheckInterval;
        long approximateCount = 0L;
        for (int index = 0; index < INGESTION_STRIPE_COUNT; index++) {
            approximateCount += publishedStripeSizes.get(index);
        }
        if (approximateCount >= maxInMemoryEntries) {
            rotationRequested = true;
        }
    }

    private void resetCountSampling() {
        stripeEntryCounts = new int[INGESTION_STRIPE_COUNT];
        publishedStripeSizes = new AtomicIntegerArray(INGESTION_STRIPE_COUNT);
        nextCountChecks = new int[INGESTION_STRIPE_COUNT];
        Arrays.fill(nextCountChecks, countCheckInterval);
    }

    private int entryCountLocked() {
        int count = 0;
        for (Map<K, V> map : entries) {
            count = Math.addExact(count, map.size());
        }
        return count;
    }

    private List<Map<K, V>> newMaps() {
        final int capacity = 1
                + (initialMapCapacity - 1) / INGESTION_STRIPE_COUNT;
        final List<Map<K, V>> maps = new ArrayList<>(INGESTION_STRIPE_COUNT);
        for (int index = 0; index < INGESTION_STRIPE_COUNT; index++) {
            maps.add(new HashMap<>(capacity));
        }
        return maps;
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
        controlLock.lock();
        try {
            recordFlushFailureLocked(failure);
        } finally {
            controlLock.unlock();
        }
    }

    private void recordFlushFailureLocked(final IndexException failure) {
        flushFailure = failure;
        accepting = false;
        paused = false;
        finishFlushLocked();
    }
}
