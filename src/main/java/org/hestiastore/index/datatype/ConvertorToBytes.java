package org.hestiastore.index.datatype;

import org.hestiastore.index.ByteSequence;

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

    ByteSequence toBytesBuffer(T object);

}
