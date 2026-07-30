package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class UnsignedByteLengthWriterTest {

    @Test
    void write_accepts255ByteBoundary() {
        final UnsignedByteLengthWriter<String> writer = createWriter();
        final ByteArrayWriter fileWriter = new ByteArrayWriter();

        final int written = writer.write(fileWriter, "a".repeat(255));
        final byte[] frame = fileWriter.toByteArray();

        assertEquals(256, written);
        assertEquals(255, frame[0] & 0xFF);
        assertEquals(256, frame.length);
    }

    @Test
    void write_rejects256BytePayloadWithoutWritingPartialFrame() {
        final UnsignedByteLengthWriter<String> writer = createWriter();
        final ByteArrayWriter fileWriter = new ByteArrayWriter();

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> writer.write(fileWriter, "a".repeat(256)));

        assertEquals("Encoded payload length '256' exceeds maximum '255'.",
                error.getMessage());
        assertEquals(0, fileWriter.toByteArray().length);
    }

    @Test
    void write_writesZeroLengthPrefix() {
        final UnsignedByteLengthWriter<String> writer = createWriter();
        final ByteArrayWriter fileWriter = new ByteArrayWriter();

        final int written = writer.write(fileWriter, "");

        assertEquals(1, written);
        assertEquals(0, fileWriter.toByteArray()[0]);
    }

    @Test
    void write_doesNotWriteWhenEncoderFails() {
        final TypeEncoder<String> failingEncoder = (value,
                reusableBuffer) -> {
                    throw new IllegalArgumentException("bad payload");
                };
        final UnsignedByteLengthWriter<String> writer = new UnsignedByteLengthWriter<>(
                failingEncoder);
        final ByteArrayWriter fileWriter = new ByteArrayWriter();

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> writer.write(fileWriter, "value"));

        assertEquals("bad payload", error.getMessage());
        assertEquals(0, fileWriter.toByteArray().length);
    }

    @Test
    void write_rejectsNullWriterBeforeEncoding() {
        final UnsignedByteLengthWriter<String> writer = createWriter();

        assertThrows(IllegalArgumentException.class,
                () -> writer.write(null, "value"));
    }

    @Test
    void constructorRejectsNullEncoder() {
        assertThrows(IllegalArgumentException.class,
                () -> new UnsignedByteLengthWriter<String>(null));
    }

    private static UnsignedByteLengthWriter<String> createWriter() {
        return new UnsignedByteLengthWriter<>(Utf8StringCodec.INSTANCE);
    }
}
