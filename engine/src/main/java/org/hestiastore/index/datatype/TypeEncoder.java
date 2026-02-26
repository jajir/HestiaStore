package org.hestiastore.index.datatype;

import java.util.Arrays;

import org.hestiastore.index.Vldtn;

/**
 * Encodes values into binary representation.
 *
 * @param <T> encoded type
 */
public interface TypeEncoder<T> {

    int INITIAL_DYNAMIC_BUFFER_SIZE = 64;

    /**
     * @param value value to encode
     * @return exact encoded size in bytes
     */
    int bytesLength(T value);

    /**
     * Encodes value into destination array.
     *
     * @param value value to encode
     * @param destination destination buffer
     * @return number of bytes written
     */
    int toBytes(T value, byte[] destination);

    /**
     * Utility helper that allocates a new byte array and fills it via the
     * encoder contract.
     *
     * @param encoder encoder instance
     * @param value value to encode
     * @param <T> value type
     * @return newly allocated encoded bytes
     */
    static <T> byte[] toByteArray(final TypeEncoder<T> encoder, final T value) {
        final TypeEncoder<T> validatedEncoder = Vldtn.requireNonNull(encoder,
                "encoder");
        final int length = validatedEncoder.bytesLength(value);
        if (length >= 0) {
            final byte[] out = new byte[length];
            final int written = validatedEncoder.toBytes(value, out);
            if (written != length) {
                throw new IllegalStateException(String.format(
                        "Encoder wrote '%s' bytes but declared '%s'", written,
                        length));
            }
            return out;
        }

        int capacity = INITIAL_DYNAMIC_BUFFER_SIZE;
        while (true) {
            final byte[] out = new byte[capacity];
            try {
                final int written = validatedEncoder.toBytes(value, out);
                if (written < 0 || written > out.length) {
                    throw new IllegalStateException(String.format(
                            "Encoder wrote invalid number of bytes '%s' for buffer size '%s'",
                            written, out.length));
                }
                return written == out.length ? out
                        : Arrays.copyOf(out, written);
            } catch (final IllegalArgumentException e) {
                if (!isDestinationBufferTooSmall(e)) {
                    throw e;
                }
                if (capacity == Integer.MAX_VALUE) {
                    throw new IllegalArgumentException(
                            "Unable to encode value into byte array.",
                            e);
                }
                capacity = capacity > Integer.MAX_VALUE / 2 ? Integer.MAX_VALUE
                        : capacity * 2;
            }
        }
    }

    private static boolean isDestinationBufferTooSmall(
            final IllegalArgumentException error) {
        final String message = error.getMessage();
        return message != null && message.contains("Destination buffer too small");
    }
}
