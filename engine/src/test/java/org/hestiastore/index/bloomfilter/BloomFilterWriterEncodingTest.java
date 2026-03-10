package org.hestiastore.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.datatype.EncodedBytes;
import org.hestiastore.index.datatype.TypeEncoder;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class BloomFilterWriterEncodingTest {

    private static final int DISK_IO_BUFFER_SIZE = 1024;

    @Test
    void write_rejectsNonPositiveDeclaredLength() {
        try (final BloomFilterWriter<String> writer = new BloomFilterWriter<>(
                new TypeEncoder<String>() {
                    @Override
                    public EncodedBytes encode(final String value,
                            final byte[] reusableBuffer) {
                        return new EncodedBytes(reusableBuffer, 0);
                    }
                }, new Hash(new BitArray(16), 2), new MemDirectory(), "test.bf",
                DISK_IO_BUFFER_SIZE)) {
            final IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class, () -> writer.write("k"));
            assertTrue(error.getMessage().contains("encodedLength"));
        }
    }

    @Test
    void write_rejectsEncodedLengthGreaterThanArraySize() {
        try (final BloomFilterWriter<String> writer = new BloomFilterWriter<>(
                new TypeEncoder<String>() {
                    @Override
                    public EncodedBytes encode(final String value,
                            final byte[] reusableBuffer) {
                        return new EncodedBytes(new byte[2], 3);
                    }
                }, new Hash(new BitArray(16), 2), new MemDirectory(), "test.bf",
                DISK_IO_BUFFER_SIZE)) {
            final IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class, () -> writer.write("abc"));
            assertTrue(error.getMessage().contains("exceeds byte array size"));
        }
    }
}
