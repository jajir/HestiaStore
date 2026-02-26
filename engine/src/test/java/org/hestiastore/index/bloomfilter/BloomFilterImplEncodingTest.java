package org.hestiastore.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

import org.hestiastore.index.datatype.TypeEncoder;
import org.hestiastore.index.datatype.TestEncoding;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class BloomFilterImplEncodingTest {

    private static final int HASH_FUNCTIONS = 2;
    private static final int INDEX_SIZE_IN_BYTES = 256;
    private static final int DISK_IO_BUFFER_SIZE = 1024;

    @Test
    void isNotStored_rejectsNegativeLengthEncoder() {
        final NegativeLengthEncoder encoder = new NegativeLengthEncoder();
        final BloomFilterImpl<String> bloomFilter = new BloomFilterImpl<>(
                new MemDirectory(), "test.bf", HASH_FUNCTIONS,
                INDEX_SIZE_IN_BYTES, encoder, "segment-001",
                DISK_IO_BUFFER_SIZE);
        bloomFilter.setNewHash(new Hash(new BitArray(INDEX_SIZE_IN_BYTES),
                HASH_FUNCTIONS));

        assertThrows(IllegalArgumentException.class,
                () -> bloomFilter.isNotStored("negative-length-key"));
    }

    @Test
    void isNotStored_resizesReusableBufferForLargeKey() throws Exception {
        final String key = "x".repeat(128);
        final FixedAsciiEncoder encoder = new FixedAsciiEncoder();
        final BloomFilterImpl<String> bloomFilter = new BloomFilterImpl<>(
                new MemDirectory(), "test.bf", HASH_FUNCTIONS,
                INDEX_SIZE_IN_BYTES, encoder, "segment-001",
                DISK_IO_BUFFER_SIZE);
        final ThreadLocal<byte[]> reusableBuffer = getReusableBuffer(
                bloomFilter);
        final Hash hash = new Hash(new BitArray(INDEX_SIZE_IN_BYTES),
                HASH_FUNCTIONS);

        assertEquals(64, reusableBuffer.get().length);
        hash.store(TestEncoding.toByteArray(encoder, key));
        bloomFilter.setNewHash(hash);

        assertFalse(bloomFilter.isNotStored(key));
        assertTrue(reusableBuffer.get().length >= key.length());
    }

    @SuppressWarnings("unchecked")
    private ThreadLocal<byte[]> getReusableBuffer(
            final BloomFilterImpl<String> bloomFilter) throws Exception {
        final Field field = BloomFilterImpl.class
                .getDeclaredField("reusableBytesBuffer");
        field.setAccessible(true);
        return (ThreadLocal<byte[]>) field.get(bloomFilter);
    }

    private static final class NegativeLengthEncoder
            implements TypeEncoder<String> {

        @Override
        public int bytesLength(final String value) {
            return -1;
        }

        @Override
        public int toBytes(final String value, final byte[] destination) {
            throw new IllegalStateException("must not be called");
        }
    }

    private static final class FixedAsciiEncoder implements TypeEncoder<String> {

        @Override
        public int bytesLength(final String value) {
            return value.length();
        }

        @Override
        public int toBytes(final String value, final byte[] destination) {
            final byte[] bytes = value.getBytes(StandardCharsets.ISO_8859_1);
            if (destination.length < bytes.length) {
                throw new IllegalArgumentException(
                        "Destination buffer too small.");
            }
            System.arraycopy(bytes, 0, destination, 0, bytes.length);
            return bytes.length;
        }
    }
}
