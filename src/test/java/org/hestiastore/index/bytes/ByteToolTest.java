package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ByteToolTest {

    private static final ByteSequence BYTES_EMPTY = ByteSequences
            .wrap("".getBytes());
    private static final ByteSequence BYTES_AHOJ = ByteSequences
            .wrap("ahoj".getBytes());

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
        final ByteSequence a1 = ByteSequences.wrap(a.getBytes());
        final ByteSequence b1 = ByteSequences.wrap(b.getBytes());
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
        final ByteSequenceView input = (ByteSequenceView) ByteSequences
                .wrap(new byte[] { 0, 1, 2 });

        assertSame(input, ByteTool.getRemainingBytesAfterIndex(0, input));
    }

    @Test
    void test_getRemainingBytesAfterIndex_fullLengthReturnsEmpty() {
        final ByteSequenceView input = (ByteSequenceView) ByteSequences
                .wrap(new byte[] { 0, 1, 2 });

        assertSame(ByteSequence.EMPTY,
                ByteTool.getRemainingBytesAfterIndex(input.length(), input));
    }

    @Test
    void test_getRemainingBytesAfterIndex_returnsViewForBytes() {
        final ByteSequenceView input = (ByteSequenceView) ByteSequences
                .wrap(new byte[] { 1, 2, 3, 4 });

        final ByteSequence remainder = ByteTool.getRemainingBytesAfterIndex(2,
                input);

        assertTrue(remainder instanceof ByteSequenceSlice);
        assertArrayEquals(new byte[] { 3, 4 }, remainder.toByteArray());
    }

    @Test
    void test_getRemainingBytesAfterIndex_handlesConcatenatedSequence() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2, 3 }),
                ByteSequences.wrap(new byte[] { 4, 5 }));

        final ByteSequence remainder = ByteTool.getRemainingBytesAfterIndex(2,
                concatenated);

        assertTrue(remainder instanceof ConcatenatedByteSequence);
        assertArrayEquals(new byte[] { 3, 4, 5 }, remainder.toByteArray());
    }

    private void testFunction(final int sharedLength, final String str,
            final String expectedResult) {
        final ByteSequence input = ByteSequences.wrap(str.getBytes());
        final ByteSequence wrapped = ByteTool
                .getRemainingBytesAfterIndex(sharedLength, input);
        assertEquals(expectedResult, new String(wrapped.toByteArray()));
    }

    @Test
    void test_concatenate() {
        final ByteSequence first = ByteSequences.wrap(new byte[] { 1, 2 });
        final ByteSequence second = ByteSequences.wrap(new byte[] { 3, 4, 5 });
        final ByteSequence result = ByteTool.concatenate(first, second);

        assertTrue(result instanceof ConcatenatedByteSequence);
        assertEquals(5, result.length());
        assertArrayEquals(new byte[] { 1, 2, 3, 4, 5 }, result.toByteArray());
    }

    @Test
    void test_concatenateShortCircuitsEmptyOperands() {
        final ByteSequence single = ByteSequences.wrap(new byte[] { 9 });
        final ByteSequence first = ByteTool.concatenate(ByteSequence.EMPTY,
                single);
        final ByteSequence second = ByteTool.concatenate(single,
                ByteSequence.EMPTY);

        assertSame(single, first);
        assertSame(single, second);
    }

}
