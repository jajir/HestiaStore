package com.hestiastore.index;

import java.io.Closeable;

/**
 * Extends {@link Closeable} interface and remove IOException.
 * 
 * @author jan
 *
 */
public interface CloseableResource extends Closeable {

    @Override
    void close();

}
