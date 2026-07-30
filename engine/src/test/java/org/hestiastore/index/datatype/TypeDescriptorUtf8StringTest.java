package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.MemFileReader;
import org.junit.jupiter.api.Test;

class TypeDescriptorUtf8StringTest {

    private final TypeDescriptorUtf8String descriptor = new TypeDescriptorUtf8String();

    @Test
    void encoderAndDecoder_roundTripMultilingualValue() {
        final String value = "Ahoj, Καλημέρα, こんにちは, 🙂";

        final byte[] encoded = TestEncoding.toByteArray(
                descriptor.getTypeEncoder(), value);
        final String decoded = descriptor.getTypeDecoder().decode(encoded);

        assertArrayEquals(value.getBytes(StandardCharsets.UTF_8), encoded);
        assertEquals(value, decoded);
    }

    @Test
    void writerAndReader_roundTrip128EmojiCodePoints() {
        final String value = "🙂".repeat(128);
        final ByteArrayWriter writer = new ByteArrayWriter();

        final int written = descriptor.getTypeWriter().write(writer, value);
        final byte[] frame = writer.toByteArray();
        final String decoded = descriptor.getTypeReader()
                .read(new MemFileReader(frame));

        assertEquals(514, written);
        assertArrayEquals(new byte[] { (byte) 0x80, 0x04 },
                Arrays.copyOf(frame, 2));
        assertEquals(value, decoded);
    }

    @Test
    void writerAndReader_roundTrip64000EmojiCodePoints() {
        final String value = "🙂".repeat(64_000);
        final ByteArrayWriter writer = new ByteArrayWriter();

        final int written = descriptor.getTypeWriter().write(writer, value);
        final byte[] frame = writer.toByteArray();
        final String decoded = descriptor.getTypeReader()
                .read(new MemFileReader(frame));

        assertEquals(256_003, written);
        assertArrayEquals(
                new byte[] { (byte) 0x80, (byte) 0xD0, (byte) 0x0F },
                Arrays.copyOf(frame, 3));
        assertEquals(value, decoded);
    }

    @Test
    void decoderRejectsMalformedUtf8() {
        final byte[] malformed = new byte[] { (byte) 0xF0, (byte) 0x9F };

        assertThrows(IndexException.class,
                () -> descriptor.getTypeDecoder().decode(malformed));
    }

    @Test
    void descriptorContractsRemainStable() {
        final TypeDescriptorUtf8String equalDescriptor = new TypeDescriptorUtf8String();
        final TypeDescriptorTinyUtf8String differentDescriptor = new TypeDescriptorTinyUtf8String();

        assertEquals(0, descriptor.getComparator().compare("same", "same"));
        assertEquals(TypeDescriptorUtf8String.TOMBSTONE_VALUE,
                descriptor.getTombstone());
        assertEquals(descriptor, equalDescriptor);
        assertEquals(descriptor.hashCode(), equalDescriptor.hashCode());
        assertNotEquals(descriptor, differentDescriptor);
        assertFalse(descriptor.getEstimatedAverageSizeInBytes().isPresent());
    }
}
