package org.hestiastore.index.datatype;

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
        final int length = Vldtn.requireGreaterThanOrEqualToZero(
                validatedEncoder.bytesLength(value), "encodedLength");
        final byte[] out = new byte[length];
        final int written = validatedEncoder.toBytes(value, out);
        if (written != length) {
            throw new IllegalStateException(String.format(
                    "Encoder wrote '%s' bytes but declared '%s'", written,
                    length));
        }
        return out;
    }
}
