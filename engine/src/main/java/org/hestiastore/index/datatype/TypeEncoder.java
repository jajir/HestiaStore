package org.hestiastore.index.datatype;

/**
 * Encodes values into binary representation.
 *
 * @param <T> encoded type
 */
public interface TypeEncoder<T> {

    /**
     * Encodes value in a single operation and returns both payload bytes and
     * the effective encoded length.
     *
     * @param value value to encode
     * @param reusableBuffer caller-provided reusable buffer
     * @return encoded payload metadata
     */
    EncodedBytes encode(T value, byte[] reusableBuffer);
}
