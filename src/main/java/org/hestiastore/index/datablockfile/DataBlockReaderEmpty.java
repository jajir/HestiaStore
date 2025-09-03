package org.hestiastore.index.datablockfile;

/**
 * Data block reader that does not read any data.
 */
public class DataBlockReaderEmpty implements DataBlockReader {

    @Override
    public DataBlock read() {
        // It's empty reader, it doesn't read any data.
        return null;
    }

    @Override
    public void close() {
        // Intention is to provide an empty implementation
    }

}
