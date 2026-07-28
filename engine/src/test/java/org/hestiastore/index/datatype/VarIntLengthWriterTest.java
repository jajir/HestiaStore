package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

class VarIntLengthWriterTest {

    @Test
    void write_writesTwoBytePrefixFor128BytePayload() {
        final VarIntLengthWriter<String> writer = new VarIntLengthWriter<>(
                Utf8StringCodec.INSTANCE);
        final ByteArrayWriter fileWriter = new ByteArrayWriter();
        final String value = "a".repeat(128);

        final int written = writer.write(fileWriter, value);
        final byte[] bytes = fileWriter.toByteArray();

        assertEquals(130, written);
        assertArrayEquals(new byte[] { (byte) 0x80, 0x01 },
                Arrays.copyOf(bytes, 2));
        assertArrayEquals(value.getBytes(StandardCharsets.UTF_8),
                Arrays.copyOfRange(bytes, 2, bytes.length));
    }

    @Test
    void write_writesCanonicalZeroLengthPrefix() {
        final VarIntLengthWriter<String> writer = new VarIntLengthWriter<>(
                Utf8StringCodec.INSTANCE);
        final ByteArrayWriter fileWriter = new ByteArrayWriter();

        final int written = writer.write(fileWriter, "");

        assertEquals(1, written);
        assertArrayEquals(new byte[] { 0 }, fileWriter.toByteArray());
    }

    @Test
    void write_doesNotWriteWhenEncoderFails() {
        final TypeEncoder<String> failingEncoder = (value,
                reusableBuffer) -> {
                    throw new IllegalArgumentException("bad payload");
                };
        final VarIntLengthWriter<String> writer = new VarIntLengthWriter<>(
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
        final VarIntLengthWriter<String> writer = new VarIntLengthWriter<>(
                Utf8StringCodec.INSTANCE);

        assertThrows(IllegalArgumentException.class,
                () -> writer.write(null, "value"));
    }

    @Test
    void constructorRejectsNullEncoder() {
        assertThrows(IllegalArgumentException.class,
                () -> new VarIntLengthWriter<String>(null));
    }
}
