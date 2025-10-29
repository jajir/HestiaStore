package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class BytesTest {

    private static final byte[] TEST_DATA = ("Do you need real-time search"
            + " after insert? (favor Qdrant-style)").getBytes();

    @Test
    void test_storing() {
        final Bytes bytes = Bytes.of(TEST_DATA);

        assertArrayEquals(TEST_DATA, bytes.toByteArray());
    }

    @Test
    void test_equals() {
        final Bytes bytes1 = Bytes.of(TEST_DATA);
        final Bytes bytes2 = Bytes.of(TEST_DATA);

        assertEquals(bytes1, bytes2);
        assertEquals(bytes1.hashCode(), bytes2.hashCode());
    }

    @Test
    void test_slice() {
        final Bytes bytes = Bytes.of(TEST_DATA);
        final Bytes bytes2 = bytes.slice(7, 11);

        final Bytes expect = Bytes.of("need".getBytes());
        assertEquals(expect, bytes2);
        assertEquals(expect.hashCode(), bytes2.hashCode());
    }

    @Test
    void test_slice_err() {
        final Bytes bytes = Bytes.of(TEST_DATA);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> bytes.slice(7, TEST_DATA.length + 1));

        assertEquals(
                "Property 'endByte' must be between 0 and 63 (inclusive). Got: 64",
                e.getMessage());
    }

    @Test
    void test_slice_allData() {
        final Bytes bytes = Bytes.of(TEST_DATA);
        final Bytes bytes2 = bytes.slice(0, TEST_DATA.length);

        assertEquals(bytes, bytes2);
        assertEquals(bytes.hashCode(), bytes2.hashCode());
    }

    @Test
    void test_slice_first_byte() {
        final Bytes bytes = Bytes.of(TEST_DATA);
        final Bytes bytes2 = bytes.slice(0, 1);

        final Bytes expect = Bytes.of("D".getBytes());
        assertEquals(expect, bytes2);
        assertEquals(expect.hashCode(), bytes2.hashCode());
    }

    @Test
    void test_paddedTo() {
        final Bytes bytes = Bytes.of(TEST_DATA);
        final Bytes padded = bytes.paddedTo(100);

        assertEquals(100, padded.length());
        assertEquals(bytes, padded.slice(0, bytes.length()));
    }

    @Test
    void test_paddedTo_smaller_than_size() {
        final Bytes bytes = Bytes.of(TEST_DATA);
        final Bytes padded = bytes.paddedTo(TEST_DATA.length - 2);

        assertEquals(TEST_DATA.length, padded.length());
        assertEquals(bytes, padded.slice(0, bytes.length()));
    }

    @Test
    void test_paddedToNextCell() {
        final Bytes bytes = Bytes.of(TEST_DATA);
        final Bytes padded = bytes.paddedToNextCell();

        assertEquals(63, bytes.length());
        assertEquals(64, padded.length());
    }

}
