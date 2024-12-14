package com.coroptis.index.datatype;

/**
 * Instantiate object from byte array. Supposing that whole byte array is one
 * object.
 * <p>
 * Converted type instance use all bytes of byte array.
 * </p>
 * 
 * @author jan
 *
 * @param <T>
 */
public interface ConvertorFromBytes<T> {

    T fromBytes(byte[] array);

}
