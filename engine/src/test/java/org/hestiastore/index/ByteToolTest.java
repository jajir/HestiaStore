package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ByteToolTest {

    private static final byte[] BYTES_EMPTY_STR = "".getBytes();
    private static final byte[] BYTES_AHOJ_STR = "ahoj".getBytes();

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
                () -> ByteTool.countMatchingPrefixBytes(BYTES_EMPTY_STR, null));
        assertThrows(IllegalArgumentException.class,
                () -> ByteTool.countMatchingPrefixBytes(null, BYTES_EMPTY_STR));
    }

    private void testBytes(final String a, final String b,
            final int expectedBytes) {
        final byte[] a1 = a.getBytes();
        final byte[] b1 = b.getBytes();
        final int ret = ByteTool.countMatchingPrefixBytes(a1, b1);
        assertEquals(expectedBytes, ret);
    }

    @Test
    void test_getRemainingBytesAfterIndex() {
        testFunction(1, "ahoj", "hoj");
        testFunction(0, "ahoj", "ahoj");
        testFunction(4, "ahoj", "");

        assertThrows(IllegalArgumentException.class,
                () -> ByteTool.getRemainingBytesAfterIndex(5, BYTES_AHOJ_STR));
        assertThrows(IllegalArgumentException.class,
                () -> ByteTool.getRemainingBytesAfterIndex(-1, BYTES_AHOJ_STR));
        assertThrows(IllegalArgumentException.class,
                () -> ByteTool.getRemainingBytesAfterIndex(0, null));
    }

    private void testFunction(final int sharedLength, final String str,
            final String expectedResult) {
        final byte[] a1 = str.getBytes();
        final byte[] retBytes = ByteTool
                .getRemainingBytesAfterIndex(sharedLength, a1);
        final String ret = new String(retBytes);
        assertEquals(expectedResult, ret);
    }

    @Test
    void test_concatenate() {
        final byte[] first = new byte[] { 1, 2 };
        final byte[] second = new byte[] { 3, 4, 5 };
        final byte[] result = ByteTool.concatenate(first, second);

        assertEquals(5, result.length);
        assertEquals(1, result[0]);
        assertEquals(2, result[1]);
        assertEquals(3, result[2]);
        assertEquals(4, result[3]);
        assertEquals(5, result[4]);
    }

}
