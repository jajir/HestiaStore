package org.hestiastore.index.datatype;

import org.hestiastore.index.Bytes;

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

    Bytes toBytesBuffer(T object);

}
