package org.hestiastore.index.datatype;

/**
 * Encodes values into binary representation.
 *
 * @param <T> encoded type
 */
public interface TypeEncoder<T> {

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
}
