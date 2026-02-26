package org.hestiastore.index.datatype;

import org.hestiastore.index.directory.FileReader;

/**
 * Reads values of a specific type from a {@link FileReader}.
 *
 * @param <T> loaded value type
 */
public interface TypeReader<T> {

    /**
     * Reads one value from the given reader.
     *
     * @param reader source reader
     * @return loaded value; may return {@code null} for end-of-stream depending
     *         on the concrete implementation
     */
    T read(FileReader reader);

}
