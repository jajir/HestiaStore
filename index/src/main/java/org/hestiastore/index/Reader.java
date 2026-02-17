package org.hestiastore.index;

/**
 * Allows to sequentially read keys. When <code>null</code> is returned it means
 * that last key was returned.
 * 
 * @author Honza
 *
 * @param<K> readed object type
 */
public interface Reader<K> {

    /**
     * Try to read data.
     * 
     * @return Return read data when it's possible. Return <code>null</code>
     *         when there are no data.
     */
    K read();

}
