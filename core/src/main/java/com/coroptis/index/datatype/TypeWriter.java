package com.coroptis.index.datatype;

import com.coroptis.index.directory.FileWriter;

/**
 * Allows to write object of some type to storage.
 * 
 * @author jan
 *
 * @param <T> type of written object
 */
public interface TypeWriter<T> {

    /**
     * Write data to file writer.
     * 
     * @param fileWriter required object where will be data written
     * @param object     required object to write
     * @return return how many bytes was written
     */
    int write(FileWriter fileWriter, T object);

}
