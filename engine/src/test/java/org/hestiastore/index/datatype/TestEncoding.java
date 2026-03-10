package org.hestiastore.index.datatype;

import java.util.Arrays;

import org.hestiastore.index.Vldtn;

/**
 * Test-only utilities for encoding values via {@link TypeEncoder}.
 */
public final class TestEncoding {

    private TestEncoding() {
    }

    public static <T> byte[] toByteArray(final TypeEncoder<T> encoder,
            final T value) {
        final TypeEncoder<T> validatedEncoder = Vldtn.requireNonNull(encoder,
                "encoder");
        final EncodedBytes encoded = Vldtn.requireNonNull(
                validatedEncoder.encode(value, new byte[0]), "encoded");
        final int length = Vldtn.requireGreaterThanOrEqualToZero(
                encoded.getLength(), "encodedLength");
        if (encoded.getBytes().length == length) {
            return encoded.getBytes();
        }
        return Arrays.copyOf(encoded.getBytes(), length);
    }
}
