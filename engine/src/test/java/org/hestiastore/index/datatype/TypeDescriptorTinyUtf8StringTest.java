package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.MemFileReader;
import org.junit.jupiter.api.Test;

class TypeDescriptorTinyUtf8StringTest {

    private final TypeDescriptorTinyUtf8String descriptor = new TypeDescriptorTinyUtf8String();

    @Test
    void writerAndReader_roundTripUnicodeWithinByteLimit() {
        final String value = "🙂".repeat(63);
        final ByteArrayWriter writer = new ByteArrayWriter();

        final int written = descriptor.getTypeWriter().write(writer, value);
        final byte[] frame = writer.toByteArray();
        final String decoded = descriptor.getTypeReader()
                .read(new MemFileReader(frame));

        assertEquals(253, written);
        assertEquals(252, frame[0] & 0xFF);
        assertEquals(value, decoded);
    }

    @Test
    void writerAndReader_accept255EncodedBytes() {
        final String value = "a".repeat(255);
        final ByteArrayWriter writer = new ByteArrayWriter();

        final int written = descriptor.getTypeWriter().write(writer, value);
        final String decoded = descriptor.getTypeReader()
                .read(new MemFileReader(writer.toByteArray()));

        assertEquals(256, written);
        assertEquals(value, decoded);
    }

    @Test
    void writerRejects256AsciiBytes() {
        final ByteArrayWriter writer = new ByteArrayWriter();

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> descriptor.getTypeWriter().write(writer,
                        "a".repeat(256)));

        assertEquals("Encoded payload length '256' exceeds maximum '255'.",
                error.getMessage());
    }

    @Test
    void writerRejects64EmojiBecauseLimitIsEncodedBytes() {
        final ByteArrayWriter writer = new ByteArrayWriter();

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> descriptor.getTypeWriter().write(writer,
                        "🙂".repeat(64)));

        assertEquals("Encoded payload length '256' exceeds maximum '255'.",
                error.getMessage());
    }

    @Test
    void decoderRejectsMalformedUtf8() {
        final byte[] malformed = new byte[] { (byte) 0xC0, (byte) 0xAF };

        assertThrows(IndexException.class,
                () -> descriptor.getTypeDecoder().decode(malformed));
    }

    @Test
    void estimatedSizeIncludesPrefixAndMaximumPayload() {
        assertEquals(256,
                descriptor.getEstimatedAverageSizeInBytes().orElseThrow());
    }
}
