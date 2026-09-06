package org.hestiastore.index.senku;

import static org.junit.jupiter.api.Assertions.assertSame;

import org.hestiastore.index.datatype.NullValue;
import org.junit.jupiter.api.Test;

class SenkuMergeFunctionsTest {

    @Test
    void explicitSetReducerHasStableIdentityAndCanonicalValue() {
        assertSame(SenkuMergeFunctions.longSet(),
                SenkuMergeFunctions.longSet());
        assertSame(NullValue.NULL, SenkuMergeFunctions.longSet().apply(1L,
                NullValue.NULL, NullValue.NULL));
        assertSame(NullValue.NULL, SenkuMergeFunctions.longSet().apply(0L,
                NullValue.TOMBSTONE, NullValue.NULL));
    }
}
