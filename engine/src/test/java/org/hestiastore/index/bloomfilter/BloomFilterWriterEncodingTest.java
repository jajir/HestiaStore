package org.hestiastore.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.datatype.TypeEncoder;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class BloomFilterWriterEncodingTest {

    private static final int DISK_IO_BUFFER_SIZE = 1024;

    @Test
    void write_rejectsNonPositiveDeclaredLength() {
        final BloomFilterWriter<String> writer = new BloomFilterWriter<>(
                new TypeEncoder<String>() {
                    @Override
                    public int bytesLength(final String value) {
                        return 0;
                    }

                    @Override
                    public int toBytes(final String value,
                            final byte[] destination) {
                        return 0;
                    }
                }, new Hash(new BitArray(16), 2), new MemDirectory(),
                "test.bf", DISK_IO_BUFFER_SIZE);

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, () -> writer.write("k"));
        assertTrue(error.getMessage().contains("encodedLength"));
    }

    @Test
    void write_rejectsDeclaredAndWrittenLengthMismatch() {
        final BloomFilterWriter<String> writer = new BloomFilterWriter<>(
                new TypeEncoder<String>() {
                    @Override
                    public int bytesLength(final String value) {
                        return 3;
                    }

                    @Override
                    public int toBytes(final String value,
                            final byte[] destination) {
                        destination[0] = 'a';
                        destination[1] = 'b';
                        return 2;
                    }
                }, new Hash(new BitArray(16), 2), new MemDirectory(),
                "test.bf", DISK_IO_BUFFER_SIZE);

        final IllegalStateException error = assertThrows(
                IllegalStateException.class, () -> writer.write("abc"));
        assertTrue(error.getMessage().contains("declared"));
    }
}
