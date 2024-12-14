package com.coroptis.index.datatype;

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

}
