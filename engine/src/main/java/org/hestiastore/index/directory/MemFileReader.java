package org.hestiastore.index.directory;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;

public class MemFileReader extends AbstractCloseableResource
        implements FileReader {

    private final ByteSequence data;

    private int position;

    public MemFileReader(final byte[] data) {
        this(ByteSequences.wrap(Vldtn.requireNonNull(data, "data")));
    }

    /**
     * Creates an in-memory reader over a byte sequence.
     *
     * @param data required byte sequence
     */
    public MemFileReader(final ByteSequence data) {
        this.data = Vldtn.requireNonNull(data, "data");
        this.position = 0;
    }

    @Override
    protected void doClose() {
        position = -1;
    }

    @Override
    public int read() {
        if (position < data.length()) {
            return data.getByte(position++) & 0xFF;
        } else {
            return -1;
        }
    }

    @Override
    public int read(final byte[] bytes) {
        return read(bytes, 0, bytes.length);
    }

    @Override
    public int read(final byte[] bytes, final int offset, final int length) {
        Vldtn.requireNonNull(bytes, "bytes");
        if (offset < 0 || length < 0 || offset > bytes.length
                || offset + length > bytes.length) {
            throw new IllegalArgumentException(String.format(
                    "Range [%d, %d) exceeds array length %d", offset,
                    offset + length, bytes.length));
        }
        if (length == 0) {
            return 0;
        }
        if (position < data.length()) {
            // at least one byte will be read
            int newPosition = position + length;
            if (newPosition > data.length()) {
                newPosition = data.length();
            }
            final int toReadBytes = newPosition - position;
            ByteSequences.copy(data, position, bytes, offset, toReadBytes);
            position = newPosition;
            return toReadBytes;
        } else {
            return -1;
        }
    }

    protected int getDataLength() {
        return data.length();
    }

    protected void setPosition(final long position) {
        this.position = (int) position;
    }

    @Override
    public void skip(final long newPosition) {
        this.position = this.position + (int) newPosition;
    }
}
