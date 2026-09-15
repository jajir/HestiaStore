package org.hestiastore.index.senku;

/** Caller-confined cursor over the immutable arrays of one weighted summary. */
final class SenkuLongSummaryCursor {

    private final long[] keys;
    private final long[] weights;
    private int position;

    SenkuLongSummaryCursor(final long[] keys, final long[] weights) {
        this.keys = keys;
        this.weights = weights;
    }

    long key() {
        return keys[position];
    }

    long weight() {
        return weights[position];
    }

    boolean advance() {
        position++;
        return position < keys.length;
    }
}
