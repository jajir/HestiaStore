package org.hestiastore.index.datatype;

import static org.hestiastore.index.datatype.NullValue.NULL;
import static org.hestiastore.index.datatype.NullValue.TOMBSTONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

public class TypeDescriptorNullTest {

    private static final TypeDescriptor<NullValue> TDN = new TypeDescriptorNull();

    @Test
    void test_readWrite() {
        testReadWrite(TDN, NULL);
    }

    /*
     * This is little bit tricky and future problem. Records can't be deleted
     * because NULL value doesn't support tombstones.
     */
    @Test
    void test_readWrite_tombstone() {
        testReadWrite(TDN, NULL);
        final byte[] bytes = TDN.getConvertorToBytes().toBytes(TOMBSTONE);

        final NullValue readValue = TDN.getConvertorFromBytes()
                .fromBytes(bytes);

        assertNotEquals(TOMBSTONE, readValue);
        assertEquals(NULL, readValue);
    }

    private void testReadWrite(final TypeDescriptor<NullValue> typeDescriptor,
            final NullValue value) {

        final byte[] bytes = typeDescriptor.getConvertorToBytes()
                .toBytes(value);

        final NullValue readValue = typeDescriptor.getConvertorFromBytes()
                .fromBytes(bytes);

        assertEquals(value, readValue);
    }

}
