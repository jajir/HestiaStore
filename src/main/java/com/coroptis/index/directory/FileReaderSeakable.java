package com.coroptis.index.directory;

public interface FileReaderSeakable extends FileReader {

    /**
     * Allows to set position for file reading on specific byte.
     * 
     * @param position required position in file
     * @throws IllegalArgumentException when position is smaller that 0 or
     *                                  exceed file size
     */
    void seek(long position);

}
