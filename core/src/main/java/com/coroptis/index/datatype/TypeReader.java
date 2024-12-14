package com.coroptis.index.datatype;

import com.coroptis.index.directory.FileReader;

/**
 * Read data type instance from file reader.
 * 
 * @param <T> data type of loaded object
 */
public interface TypeReader<T> {

    /**
     * Read object from given file reader.
     * 
     * @param reader required file reader
     * @return Loaded object when it's possible otherwise return
     *         <code>null</code>.
     */
    T read(FileReader reader);

}
