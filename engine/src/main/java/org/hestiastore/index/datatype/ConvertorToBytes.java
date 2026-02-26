package org.hestiastore.index.datatype;

/**
 * Convert object of some type into byte array.
 * <p>
 * Converted type instance use all bytes of byte array.
 * </p>
 * 
 * @author jan
 *
 * @param <T>
 */
public interface ConvertorToBytes<T> {

    byte[] toBytes(T object);

    /**
     * Returns exact number of bytes required to serialize the given object
     * without allocating a temporary byte array.
     *
     * <p>
     * Implementations that can't provide size without allocating should return
     * {@code -1}.
     * </p>
     *
     * @param object value to serialize
     * @return required number of bytes, or {@code -1} when unsupported
     */
    default int bytesLength(final T object) {
        return -1;
    }

    /**
     * Serializes the given object into caller-provided buffer.
     *
     * <p>
     * Implementations that don't override this method fall back to allocating
     * via {@link #toBytes(Object)}.
     * </p>
     *
     * @param object value to serialize
     * @param destination destination byte buffer
     * @return number of bytes written into destination
     */
    default int toBytes(final T object, final byte[] destination) {
        final byte[] data = toBytes(object);
        if (data.length > destination.length) {
            throw new IllegalArgumentException(String.format(
                    "Destination buffer too small. Required '%s' but was '%s'",
                    data.length, destination.length));
        }
        System.arraycopy(data, 0, destination, 0, data.length);
        return data.length;
    }

}
