package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;

/**
 * Decodes values from binary representation.
 *
 * @param <T> decoded type
 */
public interface TypeDecoder<T> {

    /**
     * Decodes value from whole byte array.
     *
     * @param source source bytes
     * @return decoded value
     */
    T decode(byte[] source);

    /**
     * Decodes value from given byte range.
     *
     * @param source source bytes
     * @param offset offset into source
     * @param length number of bytes to decode
     * @return decoded value
     */
    default T decode(final byte[] source, final int offset, final int length) {
        final byte[] validatedSource = Vldtn.requireNonNull(source, "source");
        if (offset < 0 || length < 0
                || offset + length > validatedSource.length) {
            throw new IllegalArgumentException(String.format(
                    "Invalid range offset='%s', length='%s' for source length '%s'",
                    offset, length, validatedSource.length));
        }
        if (offset == 0 && length == validatedSource.length) {
            return decode(validatedSource);
        }
        final byte[] slice = new byte[length];
        System.arraycopy(validatedSource, offset, slice, 0, length);
        return decode(slice);
    }
}
