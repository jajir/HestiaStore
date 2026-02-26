package org.hestiastore.index.datatype;

import org.hestiastore.index.directory.FileWriter;

/**
 * Writes values of a specific type to a {@link FileWriter}.
 *
 * @param <T> written value type
 */
public interface TypeWriter<T> {

    /**
     * Writes one value to the target writer.
     *
     * @param fileWriter target writer
     * @param object value to write
     * @return number of bytes written
     */
    int write(FileWriter fileWriter, T object);

}
