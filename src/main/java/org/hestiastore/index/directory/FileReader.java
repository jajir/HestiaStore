package org.hestiastore.index.directory;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.CloseableResource;

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
     * Read all bytes into the provided Bytes instance.
     *
     * @param bytes required Bytes wrapper
     * @return number of read bytes or -1 when end of file was reached
     */
    int read(Bytes bytes);

    /**
     * Skip n bytes to specific position in file.
     *
     * @param position
     */
    void skip(long position);

}
