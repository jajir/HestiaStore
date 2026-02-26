package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

class TypeDescriptorStringConvertorTest {

    private static final List<String> VALUES = List.of("", "Ahoj", "Málaga",
            "Ahoj-€-🙂");

    @Test
    void testTypeDescriptorString_convertor_matchesIso88591Encoding() {
        assertMatchesEncoding(new TypeDescriptorString().getConvertorToBytes());
    }

    @Test
    void testTypeDescriptorShortString_convertor_matchesIso88591Encoding() {
        assertMatchesEncoding(
                new TypeDescriptorShortString().getConvertorToBytes());
    }

    @Test
    void testDestinationBufferTooSmall() {
        final ConvertorToBytes<String> convertor = new TypeDescriptorString()
                .getConvertorToBytes();
        assertThrows(IllegalArgumentException.class,
                () -> convertor.toBytes("abcd", new byte[3]));
    }

    private void assertMatchesEncoding(final ConvertorToBytes<String> convertor) {
        VALUES.forEach(value -> {
            final byte[] expected = value.getBytes(StandardCharsets.ISO_8859_1);
            assertArrayEquals(expected, convertor.toBytes(value));
            assertEquals(expected.length, convertor.bytesLength(value));

            final byte[] destination = new byte[Math.max(1, expected.length + 2)];
            Arrays.fill(destination, (byte) 0x7F);
            final int written = convertor.toBytes(value, destination);
            assertEquals(expected.length, written);
            assertArrayEquals(expected, Arrays.copyOf(destination, written));
        });
    }
}
