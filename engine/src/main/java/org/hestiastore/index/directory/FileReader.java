package org.hestiastore.index.directory;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.Vldtn;

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
    int read(byte[] bytes);

    /**
     * Read up to {@code length} bytes to given array range.
     *
     * @param bytes  required destination array
     * @param offset required destination offset (inclusive)
     * @param length required maximum number of bytes to read
     * @return number of read bytes or {@code -1} when end of file was reached
     *         before reading any byte
     */
    default int read(final byte[] bytes, final int offset, final int length) {
        final byte[] validated = Vldtn.requireNonNull(bytes, "bytes");
        if (offset < 0 || length < 0 || offset > validated.length
                || offset + length > validated.length) {
            throw new IllegalArgumentException(String.format(
                    "Range [%d, %d) exceeds array length %d", offset,
                    offset + length, validated.length));
        }
        if (length == 0) {
            return 0;
        }
        int readBytes = 0;
        while (readBytes < length) {
            final int value = read();
            if (value < 0) {
                return readBytes == 0 ? -1 : readBytes;
            }
            validated[offset + readBytes] = (byte) value;
            readBytes++;
        }
        return readBytes;
    }

    /**
     * Skip n bytes to specific position in file.
     *
     * @param position
     */
    void skip(long position);

}
