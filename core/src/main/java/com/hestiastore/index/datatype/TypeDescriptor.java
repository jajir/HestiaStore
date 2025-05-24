package com.hestiastore.index.datatype;

import java.util.Comparator;

/**
 * Describe index type. Depending on usage of type some methods doesn't have to
 * be implemented. When type is used as value type than sorting will not be
 * used.
 * 
 * @author honza
 *
 * @param <T> Described type
 */
public interface TypeDescriptor<T> {

    /**
     * Comparator of value.
     * 
     * @return
     */
    Comparator<T> getComparator();

    TypeReader<T> getTypeReader();

    TypeWriter<T> getTypeWriter();

    ConvertorFromBytes<T> getConvertorFromBytes();

    ConvertorToBytes<T> getConvertorToBytes();

    /**
     * Simple naive tombstone record implementation. Tombstone is special recrd
     * that say this key was deleted. This implementaion choose one value and
     * use it as thomstone. This value can't be used by user (he he).
     * 
     * @return
     */
    T getTombstone();

    /**
     * Varify if given value is thombstone.
     * 
     * @param value
     * @return return <code>true</code> when given value is equal to thomstone
     *         value otherwise return false
     */
    default boolean isTombstone(final T value) {
        if (value == null) {
            return false;
        }
        return getTombstone().equals(value);
    }

}
