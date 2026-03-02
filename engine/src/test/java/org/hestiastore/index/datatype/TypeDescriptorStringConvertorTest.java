package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

class TypeDescriptorStringConvertorTest {

    private static final List<String> VALUES = List.of("", "Ahoj", "Málaga",
            "Ahoj-€-🙂");

    @Test
    void testTypeDescriptorString_convertor_matchesIso88591Encoding() {
        assertMatchesEncoding(new TypeDescriptorString().getTypeEncoder());
    }

    @Test
    void testTypeDescriptorShortString_convertor_matchesIso88591Encoding() {
        assertMatchesEncoding(
                new TypeDescriptorShortString().getTypeEncoder());
    }

    @Test
    void testSurrogatePairFitsIntoSingleDestinationByte() {
        final TypeEncoder<String> convertor = new TypeDescriptorString()
                .getTypeEncoder();
        final EncodedBytes encoded = convertor.encode("🙂", new byte[1]);

        assertEquals(1, encoded.getLength());
        assertArrayEquals(new byte[] { '?' },
                Arrays.copyOf(encoded.getBytes(), encoded.getLength()));
    }

    @Test
    void testEncodeResizesSmallReusableBuffer() {
        final TypeEncoder<String> convertor = new TypeDescriptorString()
                .getTypeEncoder();
        final String value = "Ahoj-€-🙂";
        final byte[] expected = value.getBytes(StandardCharsets.ISO_8859_1);

        final EncodedBytes encoded = convertor.encode(value, new byte[2]);

        assertEquals(expected.length, encoded.getLength());
        assertArrayEquals(expected, Arrays.copyOf(encoded.getBytes(),
                encoded.getLength()));
    }

    private void assertMatchesEncoding(final TypeEncoder<String> convertor) {
        VALUES.forEach(value -> {
            final byte[] expected = value.getBytes(StandardCharsets.ISO_8859_1);
            final EncodedBytes encoded = convertor.encode(value,
                    new byte[Math.max(1, expected.length + 2)]);
            assertEquals(expected.length, encoded.getLength());
            assertArrayEquals(expected, Arrays.copyOf(encoded.getBytes(),
                    encoded.getLength()));
            assertArrayEquals(expected,
                    TestEncoding.toByteArray(convertor, value));
        });
    }
}
