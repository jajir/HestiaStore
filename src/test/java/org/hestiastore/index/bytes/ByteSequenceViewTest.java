package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class ByteSequenceViewTest {

    private static final byte[] TEST_DATA = ("Do you need real-time search"
            + " after insert? (favor Qdrant-style)").getBytes();

    @Test
    void test_storing() {
        final ByteSequence bytes = ByteSequences.wrap(TEST_DATA);

        assertArrayEquals(TEST_DATA, bytes.toByteArray());
    }

    @Test
    void test_equals() {
        final ByteSequence bytes1 = ByteSequences.wrap(TEST_DATA);
        final ByteSequence bytes2 = ByteSequences.wrap(TEST_DATA);

        assertEquals(bytes1, bytes2);
        assertEquals(bytes1.hashCode(), bytes2.hashCode());
    }

    @Test
    void test_subBytes() {
        final ByteSequence bytes = ByteSequences.wrap(TEST_DATA);
        final ByteSequence bytes2 = bytes.slice(7, 11);

        final byte[] expect = "need".getBytes();
        assertArrayEquals(expect, bytes2.toByteArray());
    }

    @Test
    void test_subBytes_err() {
        final ByteSequence bytes = ByteSequences.wrap(TEST_DATA);
        assertThrows(IllegalArgumentException.class,
                () -> bytes.slice(7, TEST_DATA.length + 1));
    }

    @Test
    void test_subBytes_allData() {
        final ByteSequence bytes = ByteSequences.wrap(TEST_DATA);
        final ByteSequence bytes2 = bytes.slice(0, TEST_DATA.length);

        assertArrayEquals(bytes.toByteArray(), bytes2.toByteArray());
    }

    @Test
    void test_subBytes_first_byte() {
        final ByteSequence bytes = ByteSequences.wrap(TEST_DATA);
        final ByteSequence bytes2 = bytes.slice(0, 1);

        final byte[] expect = "D".getBytes();
        assertArrayEquals(expect, bytes2.toByteArray());
    }

    @Test
    void test_paddedTo() {
        final ByteSequence bytes = ByteSequences.wrap(TEST_DATA);
        final ByteSequence padded = ByteSequences.padToLength(bytes, 100);

        assertEquals(100, padded.length());
        assertArrayEquals(bytes.toByteArray(),
                padded.slice(0, bytes.length()).toByteArray());
    }

    @Test
    void test_paddedTo_smaller_than_size() {
        final ByteSequence bytes = ByteSequences.wrap(TEST_DATA);
        final ByteSequence padded = ByteSequences.padToLength(bytes,
                TEST_DATA.length - 2);

        assertEquals(TEST_DATA.length, padded.length());
        assertArrayEquals(bytes.toByteArray(),
                padded.slice(0, bytes.length()).toByteArray());
    }

    @Test
    void test_paddedToNextCell() {
        final ByteSequence bytes = ByteSequences.wrap(TEST_DATA);
        final ByteSequence padded = ByteSequences.padToCell(bytes, 16);

        assertEquals(63, bytes.length());
        assertEquals(64, padded.length());
    }

    @Test
    void test_slice_returnsView() {
        final ByteSequence bytes = ByteSequences.wrap(new byte[] { 1, 2, 3, 4 });
        assertTrue(bytes instanceof ByteSequenceView);

        final ByteSequence slice = bytes.slice(1, 3);

        assertTrue(slice instanceof ByteSequenceSlice);
        assertEquals(2, slice.length());
        assertEquals(2, slice.getByte(0));
    }

    @Test
    void test_slice_zeroLengthReturnsEmptyBytes() {
        final ByteSequence bytes = ByteSequences.wrap(new byte[] { 1, 2, 3, 4 });

        final ByteSequence slice = bytes.slice(2, 2);

        assertSame(ByteSequence.EMPTY, slice);
    }

    @Test
    void copyOf_bytesReturnsSameInstance() {
        final ByteSequence bytes = ByteSequences.wrap(new byte[] { 7, 8, 9 });

        assertTrue(bytes instanceof ByteSequenceView);
        assertSame(bytes, ByteSequences.copyOf(bytes));
    }

    @Test
    void copyOf_mutableBytesReturnsImmutableView() {
        final MutableBytes mutable = MutableBytes.wrap(new byte[] { 1, 2, 3 });

        final ByteSequence copy = ByteSequences.copyOf(mutable);

        assertSame(mutable, copy);
        assertArrayEquals(new byte[] { 1, 2, 3 }, copy.toByteArray());
    }

    @Test
    void copyOf_byteSequenceViewCreatesCopy() {
        final byte[] backing = { 4, 5, 6, 7 };
        final ByteSequence view = ByteSequences.viewOf(backing, 1, 3);

        final ByteSequence copy = ByteSequences.copyOf(view);

        assertArrayEquals(new byte[] { 5, 6 }, copy.toByteArray());
        assertNotSame(backing, copy.toByteArray());
    }

}
