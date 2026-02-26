package org.hestiastore.index.datatype;

import static org.hestiastore.index.datatype.NullValue.NULL;
import static org.hestiastore.index.datatype.NullValue.TOMBSTONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TypeDescriptorNullTest {

    private static final TypeDescriptor<NullValue> TDN = new TypeDescriptorNull();

    @Test
    void test_readWrite() {
        assertTrue(true);
        testReadWrite(TDN, NULL);
    }

    /*
     * This is little bit tricky and future problem. Records can't be deleted
     * because NULL value doesn't support tombstones.
     */
    @Test
    void test_readWrite_tombstone() {
        testReadWrite(TDN, NULL);
        final byte[] bytes = TestEncoding.toByteArray(TDN.getTypeEncoder(),
                TOMBSTONE);

        final NullValue readValue = TDN.getTypeDecoder()
                .decode(bytes);

        assertNotEquals(TOMBSTONE, readValue);
        assertEquals(NULL, readValue);
    }

    private void testReadWrite(final TypeDescriptor<NullValue> typeDescriptor,
            final NullValue value) {

        final byte[] bytes = TestEncoding.toByteArray(
                typeDescriptor.getTypeEncoder(), value);

        final NullValue readValue = typeDescriptor.getTypeDecoder()
                .decode(bytes);

        assertEquals(value, readValue);
    }

}
