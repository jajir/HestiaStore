package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Stream;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class Utf8StringCodecTest {

    private static final Utf8StringCodec CODEC = Utf8StringCodec.INSTANCE;

    @ParameterizedTest
    @ValueSource(strings = { "", "plain ASCII", "Příliš žluťoučký kůň",
            "Кириллица", "漢字", "🙂", "👨‍👩‍👧‍👦", "e\u0301" })
    void encodeAndDecode_roundTripsStandardUtf8(final String value) {
        final EncodedBytes encoded = CODEC.encode(value, new byte[0]);
        final byte[] payload = Arrays.copyOf(encoded.getBytes(),
                encoded.getLength());

        assertArrayEquals(value.getBytes(StandardCharsets.UTF_8), payload);
        assertEquals(value, CODEC.decode(payload));
    }

    @Test
    void encode_reusesSufficientCallerBuffer() {
        final byte[] reusableBuffer = new byte[32];

        final EncodedBytes encoded = CODEC.encode("🙂", reusableBuffer);

        assertSame(reusableBuffer, encoded.getBytes());
        assertEquals(4, encoded.getLength());
        assertArrayEquals("🙂".getBytes(StandardCharsets.UTF_8),
                Arrays.copyOf(encoded.getBytes(), encoded.getLength()));
    }

    @Test
    void encode_growsInsufficientCallerBuffer() {
        final String value = "漢字🙂";

        final EncodedBytes encoded = CODEC.encode(value, new byte[1]);

        assertEquals(value.getBytes(StandardCharsets.UTF_8).length,
                encoded.getLength());
        assertArrayEquals(value.getBytes(StandardCharsets.UTF_8),
                Arrays.copyOf(encoded.getBytes(), encoded.getLength()));
    }

    @ParameterizedTest
    @ValueSource(strings = { "\uD800", "\uDC00" })
    void encode_rejectsUnpairedSurrogates(final String malformed) {
        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> CODEC.encode(malformed, new byte[0]));

        assertEquals("String contains malformed UTF-16 input.",
                error.getMessage());
    }

    @Test
    void encode_rejectsNullValue() {
        assertThrows(IllegalArgumentException.class,
                () -> CODEC.encode(null, new byte[0]));
    }

    @Test
    void encode_rejectsNullReusableBuffer() {
        assertThrows(IllegalArgumentException.class,
                () -> CODEC.encode("value", null));
    }

    @ParameterizedTest
    @MethodSource("malformedUtf8")
    void decode_rejectsMalformedUtf8(final byte[] malformed) {
        final IndexException error = assertThrows(IndexException.class,
                () -> CODEC.decode(malformed));

        assertEquals("Invalid UTF-8 payload.", error.getMessage());
    }

    @Test
    void decode_rejectsNullSource() {
        assertThrows(IllegalArgumentException.class,
                () -> CODEC.decode(null));
    }

    private static Stream<Arguments> malformedUtf8() {
        return Stream.of(Arguments.of((Object) new byte[] { (byte) 0xC3, 0x28 }),
                Arguments.of((Object) new byte[] { (byte) 0xF0, (byte) 0x9F }),
                Arguments.of(
                        (Object) new byte[] { (byte) 0xC0, (byte) 0xAF }),
                Arguments.of((Object) new byte[] { (byte) 0xED, (byte) 0xA0,
                        (byte) 0x80 }));
    }
}
