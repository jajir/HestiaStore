package org.hestiastore.index.datatype;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.directory.FileWriter;

/**
 * Reads and writes canonical unsigned LEB128 values in the non-negative Java
 * {@code int} range.
 */
final class UnsignedLeb128 {

    static final int MAX_ENCODED_BYTES = 5;
    static final int MAX_FRAME_PAYLOAD_BYTES = Integer.MAX_VALUE
            - MAX_ENCODED_BYTES;

    private static final int DATA_BITS_PER_BYTE = 7;
    private static final int DATA_MASK = 0x7F;
    private static final int CONTINUATION_MASK = 0x80;
    private static final int MAX_FINAL_BYTE_VALUE = 0x07;

    private UnsignedLeb128() {
    }

    /**
     * Writes one non-negative integer in canonical unsigned LEB128 form.
     *
     * @param writer target writer
     * @param value non-negative value
     * @return encoded byte count
     */
    static int write(final FileWriter writer, final int value) {
        final FileWriter validatedWriter = Vldtn.requireNonNull(writer,
                "writer");
        int remaining = Vldtn.requireGreaterThanOrEqualToZero(value, "value");
        int written = 0;
        do {
            int current = remaining & DATA_MASK;
            remaining >>>= DATA_BITS_PER_BYTE;
            if (remaining != 0) {
                current |= CONTINUATION_MASK;
            }
            validatedWriter.write((byte) current);
            written++;
        } while (remaining != 0);
        return written;
    }

    /**
     * Reads one canonical unsigned LEB128 integer.
     *
     * @param reader source reader
     * @return decoded value, or {@code -1} when EOF occurs before the prefix
     */
    static int read(final FileReader reader) {
        final FileReader validatedReader = Vldtn.requireNonNull(reader,
                "reader");
        int value = 0;
        int shift = 0;
        for (int index = 0; index < MAX_ENCODED_BYTES; index++) {
            final int current = validatedReader.read();
            if (current < 0) {
                if (index == 0) {
                    return -1;
                }
                throw new IndexException(
                        "Truncated unsigned LEB128 length prefix.");
            }
            final int data = current & DATA_MASK;
            if (index == MAX_ENCODED_BYTES - 1
                    && data > MAX_FINAL_BYTE_VALUE) {
                throw new IndexException(
                        "Unsigned LEB128 length exceeds the supported integer range.");
            }
            value |= data << shift;
            if ((current & CONTINUATION_MASK) == 0) {
                if (index > 0 && data == 0) {
                    throw new IndexException(
                            "Non-canonical unsigned LEB128 length prefix.");
                }
                return value;
            }
            shift += DATA_BITS_PER_BYTE;
        }
        throw new IndexException(
                "Unsigned LEB128 length prefix exceeds five bytes.");
    }
}
