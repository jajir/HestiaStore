package org.hestiastore.index.datablockfile;

/**
 * Interface defining a reader of data blocks.
 */
public interface Reader<T> {

    /**
     * Reads the next data block.
     *
     * Reader should be used for binary data blocks. Reader should not be used
     * for application data.
     *
     * @return the next data block, or null if there are no more data blocks to
     *         read
     */
    T read();

}
