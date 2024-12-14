package com.coroptis.index.directory;

import com.coroptis.index.CloseableResource;

/**
 * With file reader it's not possible to go back. When one byte could be read
 * just once.
 *
 * @author jajir
 * 
 */
public interface FileReader extends CloseableResource {

    /**
     * Read one byte. When byte is not available than return -1.
     *
     * @return read byte as int value from 0 to 255 (inclusive). when value -1
     *         is returned that end of file was reached.
     */
    int read();

    /**
     * Read all bytes to given field.
     *
     * @param bytes required byte array
     * @return Return number of read bytes. When it's -1 than end of file was
     *         reached.
     */
    int read(byte bytes[]);

    /**
     * Skip n bytes to specific position in file.
     *
     * @param position
     */
    void skip(long position);

}
