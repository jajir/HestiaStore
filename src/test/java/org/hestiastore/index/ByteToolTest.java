package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    private void testFunction(final int sharedLength, final String str,
            final String expectedResult) {
        final Bytes input = Bytes.of(str.getBytes());
        final Bytes wrapped = ByteTool.getRemainingBytesAfterIndex(sharedLength,
                input);
        assertEquals(expectedResult, new String(wrapped.toByteArray()));
    }

    @Test
    void test_concatenate() {
        final Bytes first = Bytes.of(new byte[] { 1, 2 });
        final Bytes second = Bytes.of(new byte[] { 3, 4, 5 });
        final Bytes result = ByteTool.concatenate(first, second);

        assertEquals(5, result.length());
        final byte[] out = result.toByteArray();
        assertEquals(1, out[0]);
        assertEquals(2, out[1]);
        assertEquals(3, out[2]);
        assertEquals(4, out[3]);
        assertEquals(5, out[4]);
    }

}
