package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void test_subBytes() {
        final Bytes bytes = Bytes.of(TEST_DATA);
        final ByteSequence bytes2 = bytes.slice(7, 11);

        final byte[] expect = "need".getBytes();
        assertArrayEquals(expect, bytes2.toByteArray());
    }

    @Test
    void test_subBytes_err() {
        final Bytes bytes = Bytes.of(TEST_DATA);
        assertThrows(IllegalArgumentException.class,
                () -> bytes.slice(7, TEST_DATA.length + 1));
    }

    @Test
    void test_subBytes_allData() {
        final Bytes bytes = Bytes.of(TEST_DATA);
        final ByteSequence bytes2 = bytes.slice(0, TEST_DATA.length);

        assertArrayEquals(bytes.toByteArray(), bytes2.toByteArray());
    }

    @Test
    void test_subBytes_first_byte() {
        final Bytes bytes = Bytes.of(TEST_DATA);
        final ByteSequence bytes2 = bytes.slice(0, 1);

        final byte[] expect = "D".getBytes();
        assertArrayEquals(expect, bytes2.toByteArray());
    }

    @Test
    void test_paddedTo() {
        final Bytes bytes = Bytes.of(TEST_DATA);
        final Bytes padded = bytes.paddedTo(100);

        assertEquals(100, padded.length());
        assertArrayEquals(bytes.toByteArray(),
                padded.slice(0, bytes.length()).toByteArray());
    }

    @Test
    void test_paddedTo_smaller_than_size() {
        final Bytes bytes = Bytes.of(TEST_DATA);
        final Bytes padded = bytes.paddedTo(TEST_DATA.length - 2);

        assertEquals(TEST_DATA.length, padded.length());
        assertArrayEquals(bytes.toByteArray(),
                padded.slice(0, bytes.length()).toByteArray());
    }

    @Test
    void test_paddedToNextCell() {
        final Bytes bytes = Bytes.of(TEST_DATA);
        final Bytes padded = bytes.paddedToNextCell();

        assertEquals(63, bytes.length());
        assertEquals(64, padded.length());
    }

    @Test
    void test_slice_returnsView() {
        final Bytes bytes = Bytes.of(new byte[] { 1, 2, 3, 4 });

        final ByteSequence slice = bytes.slice(1, 3);

        assertTrue(slice instanceof ByteSequenceView);
        final ByteSequenceView view = (ByteSequenceView) slice;
        assertEquals(2, view.length());
        assertEquals(2, view.getByte(0));
    }

    @Test
    void test_slice_zeroLengthReturnsEmptyBytes() {
        final Bytes bytes = Bytes.of(new byte[] { 1, 2, 3, 4 });

        final ByteSequence slice = bytes.slice(2, 2);

        assertSame(Bytes.EMPTY, slice);
    }

}
