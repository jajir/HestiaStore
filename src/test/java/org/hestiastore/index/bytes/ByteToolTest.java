package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ByteToolTest {

    private static final Bytes BYTES_EMPTY = Bytes.of("".getBytes());
    private static final Bytes BYTES_AHOJ = Bytes.of("ahoj".getBytes());

    @Test
    void test_countMatchingPrefixBytes() {
        testBytes("a", "a", 1);
        testBytes("aaaa", "aaaa", 4);
        testBytes("ahoj", "ahoj", 4);
        testBytes("0ahoj", "ahoj", 0);
        testBytes("ahoj", "0ahoj", 0);
        testBytes("ahoj", "ahoja", 4);
        testBytes("ahoja", "ahoj", 4);
        testBytes("ahoj Pepo", "ahoj Karle", 5);
        testBytes("", "ahoj", 0);
        testBytes("ahoj", "", 0);
        testBytes("", "", 0);

        assertThrows(IllegalArgumentException.class,
                () -> ByteTool.countMatchingPrefixBytes(BYTES_EMPTY, null));
        assertThrows(IllegalArgumentException.class,
                () -> ByteTool.countMatchingPrefixBytes(null, BYTES_EMPTY));
    }

    private void testBytes(final String a, final String b,
            final int expectedBytes) {
        final Bytes a1 = Bytes.of(a.getBytes());
        final Bytes b1 = Bytes.of(b.getBytes());
        final int ret = ByteTool.countMatchingPrefixBytes(a1, b1);
        assertEquals(expectedBytes, ret);
    }

    @Test
    void test_getRemainingBytesAfterIndex() {
        testFunction(1, "ahoj", "hoj");
        testFunction(0, "ahoj", "ahoj");
        testFunction(4, "ahoj", "");

        assertThrows(IllegalArgumentException.class,
                () -> ByteTool.getRemainingBytesAfterIndex(5, BYTES_AHOJ));
        assertThrows(IllegalArgumentException.class,
                () -> ByteTool.getRemainingBytesAfterIndex(-1, BYTES_AHOJ));
        assertThrows(IllegalArgumentException.class, () -> ByteTool
                .getRemainingBytesAfterIndex(0, (ByteSequence) null));
    }

    @Test
    void test_getRemainingBytesAfterIndex_zeroIndexReturnsOriginal() {
        final Bytes input = Bytes.of(new byte[] { 0, 1, 2 });

        assertSame(input, ByteTool.getRemainingBytesAfterIndex(0, input));
    }

    @Test
    void test_getRemainingBytesAfterIndex_fullLengthReturnsEmpty() {
        final Bytes input = Bytes.of(new byte[] { 0, 1, 2 });

        assertSame(Bytes.EMPTY,
                ByteTool.getRemainingBytesAfterIndex(input.length(), input));
    }

    @Test
    void test_getRemainingBytesAfterIndex_returnsViewForBytes() {
        final Bytes input = Bytes.of(new byte[] { 1, 2, 3, 4 });

        final ByteSequence remainder = ByteTool.getRemainingBytesAfterIndex(2,
                input);

        assertTrue(remainder instanceof ByteSequenceView);
        assertArrayEquals(new byte[] { 3, 4 }, remainder.toByteArray());
    }

    @Test
    void test_getRemainingBytesAfterIndex_handlesConcatenatedSequence() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                Bytes.of(new byte[] { 1, 2, 3 }),
                Bytes.of(new byte[] { 4, 5 }));

        final ByteSequence remainder = ByteTool.getRemainingBytesAfterIndex(2,
                concatenated);

        assertTrue(remainder instanceof ConcatenatedByteSequence);
        assertArrayEquals(new byte[] { 3, 4, 5 }, remainder.toByteArray());
    }

    private void testFunction(final int sharedLength, final String str,
            final String expectedResult) {
        final Bytes input = Bytes.of(str.getBytes());
        final ByteSequence wrapped = ByteTool
                .getRemainingBytesAfterIndex(sharedLength, input);
        assertEquals(expectedResult, new String(wrapped.toByteArray()));
    }

    @Test
    void test_concatenate() {
        final Bytes first = Bytes.of(new byte[] { 1, 2 });
        final Bytes second = Bytes.of(new byte[] { 3, 4, 5 });
        final ByteSequence result = ByteTool.concatenate(first, second);

        assertTrue(result instanceof ConcatenatedByteSequence);
        assertEquals(5, result.length());
        assertArrayEquals(new byte[] { 1, 2, 3, 4, 5 }, result.toByteArray());
    }

    @Test
    void test_concatenateShortCircuitsEmptyOperands() {
        final Bytes single = Bytes.of(new byte[] { 9 });
        final ByteSequence first = ByteTool.concatenate(Bytes.EMPTY, single);
        final ByteSequence second = ByteTool.concatenate(single, Bytes.EMPTY);

        assertSame(single, first);
        assertSame(single, second);
    }

}
