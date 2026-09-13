package org.hestiastore.index.senku.internal;

import java.util.Arrays;
import java.util.function.LongToIntFunction;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;

/**
 * Caller-local bounded routing scratch for a synchronous primitive batch. Keys
 * remain in the caller's array; only configured hashes and stripe-linked
 * indices are stored. One instance is reused for every window in a single call.
 */
final class SenkuLongBatch {
    static final int WINDOW_KEYS = 2048;
    static final int STRIPE_COUNT = 128;
    static final int MAX_GROUP_KEYS = 64;

    private final int[] hashes;
    private final int[] next;
    private final int[] heads = new int[STRIPE_COUNT];
    private long[] keys;
    private int offset;

    /** Creates routing scratch bounded independently of the input length. */
    SenkuLongBatch(final int capacity) {
        Vldtn.requireTrue(capacity > 0 && capacity <= WINDOW_KEYS,
                "Batch capacity must be between 1 and WINDOW_KEYS");
        hashes = new int[capacity];
        next = new int[capacity];
    }

    /** Validates the entire slice without overflow or partial acceptance. */
    static void validateSlice(final long[] keys, final int offset,
            final int length) {
        Vldtn.requireNonNull(keys, "keys");
        Vldtn.requireTrue(
                offset >= 0 && offset <= keys.length && length >= 0
                        && length <= keys.length - offset,
                "Batch slice must be inside the keys array");
    }

    /**
     * Computes routing before any mutation lock is acquired. A window can be
     * reordered by stripe because the pure-set contract does not preserve
     * order.
     */
    void load(final long[] values, final int from, final int length,
            final LongToIntFunction hashFunction) {
        validateSlice(values, from, length);
        Vldtn.requireTrue(length <= hashes.length,
                "Batch window exceeds scratch capacity");
        Vldtn.requireNonNull(hashFunction, "hashFunction");
        keys = values;
        offset = from;
        Arrays.fill(heads, -1);
        for (int index = 0; index < length; index++) {
            final int hash;
            try {
                hash = hashFunction.applyAsInt(values[from + index]);
            } catch (Exception e) {
                if (e instanceof IndexException) {
                    throw (IndexException) e;
                }
                throw new IndexException("Senku shard hash function failed.",
                        e);
            }
            hashes[index] = hash;
            final int stripe = SenkuIngestor.stripeFromHash(hash);
            next[index] = heads[stripe];
            heads[stripe] = index;
        }
    }

    /** Returns the first local index for a stripe, or -1 when empty. */
    int first(final int stripe) {
        return heads[stripe];
    }

    /** Returns the next local index in the same stripe, or -1 at its end. */
    int next(final int index) {
        return next[index];
    }

    /** Returns a selected key without allocating or retaining an extra copy. */
    long key(final int index) {
        return keys[offset + index];
    }

    /** Returns the already evaluated persistent-shard hash. */
    int hash(final int index) {
        return hashes[index];
    }
}
