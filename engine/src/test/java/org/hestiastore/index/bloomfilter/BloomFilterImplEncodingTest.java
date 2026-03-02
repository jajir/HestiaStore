package org.hestiastore.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.hestiastore.index.datatype.EncodedBytes;
import org.hestiastore.index.datatype.TestEncoding;
import org.hestiastore.index.datatype.TypeEncoder;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class BloomFilterImplEncodingTest {

    private static final int HASH_FUNCTIONS = 2;
    private static final int INDEX_SIZE_IN_BYTES = 256;
    private static final int DISK_IO_BUFFER_SIZE = 1024;

    @Test
    void isNotStored_rejectsNonPositiveLengthEncoder() {
        final ZeroLengthEncoder encoder = new ZeroLengthEncoder();
        try (final BloomFilterImpl<String> bloomFilter = new BloomFilterImpl<>(
                new MemDirectory(), "test.bf", HASH_FUNCTIONS,
                INDEX_SIZE_IN_BYTES, encoder, "segment-001",
                DISK_IO_BUFFER_SIZE)) {
            bloomFilter.setNewHash(new Hash(new BitArray(INDEX_SIZE_IN_BYTES),
                    HASH_FUNCTIONS));

            assertThrows(IllegalArgumentException.class,
                    () -> bloomFilter.isNotStored("zero-length-key"));
        }
    }

    @Test
    void isNotStored_resizesReusableBufferForLargeKey() throws Exception {
        final String key = "x".repeat(128);
        final FixedAsciiEncoder encoder = new FixedAsciiEncoder();
        try (final BloomFilterImpl<String> bloomFilter = new BloomFilterImpl<>(
                new MemDirectory(), "test.bf", HASH_FUNCTIONS,
                INDEX_SIZE_IN_BYTES, encoder, "segment-001",
                DISK_IO_BUFFER_SIZE)) {
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
    }

    @Test
    void isNotStored_usesSinglePassEncodeWhenEncoderOverridesIt() {
        final String key = "single-pass-lookup";
        final EncodeOnlyEncoder encoder = new EncodeOnlyEncoder();
        try (final BloomFilterImpl<String> bloomFilter = new BloomFilterImpl<>(
                new MemDirectory(), "test.bf", HASH_FUNCTIONS,
                INDEX_SIZE_IN_BYTES, encoder, "segment-001",
                DISK_IO_BUFFER_SIZE)) {
            final Hash hash = new Hash(new BitArray(INDEX_SIZE_IN_BYTES),
                    HASH_FUNCTIONS);
            hash.store(key.getBytes(StandardCharsets.ISO_8859_1));
            bloomFilter.setNewHash(hash);

            assertFalse(bloomFilter.isNotStored(key));
            assertEquals(1, encoder.getEncodeCalls());
        }
    }

    @SuppressWarnings("unchecked")
    private ThreadLocal<byte[]> getReusableBuffer(
            final BloomFilterImpl<String> bloomFilter) throws Exception {
        final Field field = BloomFilterImpl.class
                .getDeclaredField("reusableBytesBuffer");
        field.setAccessible(true);
        return (ThreadLocal<byte[]>) field.get(bloomFilter);
    }

    private static final class ZeroLengthEncoder
            implements TypeEncoder<String> {

        @Override
        public EncodedBytes encode(final String value,
                final byte[] reusableBuffer) {
            return new EncodedBytes(reusableBuffer, 0);
        }
    }

    private static final class FixedAsciiEncoder
            implements TypeEncoder<String> {

        @Override
        public EncodedBytes encode(final String value,
                final byte[] reusableBuffer) {
            final byte[] bytes = value.getBytes(StandardCharsets.ISO_8859_1);
            byte[] output = reusableBuffer;
            if (output.length < bytes.length) {
                output = new byte[bytes.length];
            }
            System.arraycopy(bytes, 0, output, 0, bytes.length);
            return new EncodedBytes(output, bytes.length);
        }
    }

    private static final class EncodeOnlyEncoder implements TypeEncoder<String> {

        private int encodeCalls;

        @Override
        public EncodedBytes encode(final String value,
                final byte[] reusableBuffer) {
            encodeCalls++;
            final byte[] bytes = value.getBytes(StandardCharsets.ISO_8859_1);
            byte[] out = reusableBuffer;
            if (out.length < bytes.length) {
                out = Arrays.copyOf(bytes, bytes.length);
            } else {
                System.arraycopy(bytes, 0, out, 0, bytes.length);
            }
            return new EncodedBytes(out, bytes.length);
        }

        int getEncodeCalls() {
            return encodeCalls;
        }
    }
}
