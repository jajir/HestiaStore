package com.coroptis.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class ByteToolTest {

    private ByteTool bt = new ByteTool();

    @Test
    void test_howMuchBytesIsSame() throws Exception {
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

        assertThrows(NullPointerException.class,
                () -> bt.howMuchBytesIsSame("".getBytes(), null));
        assertThrows(NullPointerException.class,
                () -> bt.howMuchBytesIsSame(null, "".getBytes()));
    }

    private void testBytes(final String a, final String b,
            final int expectedBytes) {
        final byte[] a1 = a.getBytes();
        final byte[] b1 = b.getBytes();
        final int ret = bt.howMuchBytesIsSame(a1, b1);
        assertEquals(expectedBytes, ret);
    }

    @Test
    public void test_getRemainingBytesAfterIndex() throws Exception {
        testFunction(1, "ahoj", "hoj");
        testFunction(0, "ahoj", "ahoj");
        testFunction(4, "ahoj", "");

        assertThrows(NegativeArraySizeException.class,
                () -> bt.getRemainingBytesAfterIndex(5, "ahoj".getBytes()));
    }

    private void testFunction(final int sharedLength, final String str,
            final String expectedResult) {
        final byte[] a1 = str.getBytes();
        final byte[] retBytes = bt.getRemainingBytesAfterIndex(sharedLength,
                a1);
        final String ret = new String(retBytes);
        assertEquals(expectedResult, ret);
    }

}
