package org.hestiastore.index.senku;

import org.hestiastore.index.datatype.NullValue;

/** Explicit built-in reducers whose semantics permit specialized execution. */
public final class SenkuMergeFunctions {

    private static final SenkuMergeFunction<Long, NullValue> LONG_SET = (key,
            first, second) -> NullValue.NULL;

    private SenkuMergeFunctions() {
        // Static reducer factory.
    }

    /**
     * Returns the singleton reducer for a pure long-key set. Selecting this
     * reducer explicitly authorizes duplicate elimination without invoking a
     * user callback or decoding an order-preserving encoded key. It always
     * returns {@link NullValue#NULL}; it does not implement deletion.
     *
     * @return the identity-stable built-in set reducer
     */
    public static SenkuMergeFunction<Long, NullValue> longSet() {
        return LONG_SET;
    }
}
