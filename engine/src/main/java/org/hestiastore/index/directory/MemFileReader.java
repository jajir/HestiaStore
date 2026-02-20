package org.hestiastore.index.directory;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;

public class MemFileReader extends AbstractCloseableResource
        implements FileReader {

    private final byte[] data;

    private int position;

    public MemFileReader(final byte[] data) {
        Vldtn.requireNonNull(data, "data");
        this.data = data;
        position = 0;
    }

    @Override
    protected void doClose() {
        position = -1;
    }

    @Override
    public int read() {
        if (position < data.length) {
            return data[position++];
        } else {
            return -1;
        }
    }

    @Override
    public int read(final byte[] bytes) {
        if (position < data.length) {
            // at least one byte will be read
            int newPosition = position + bytes.length;
            if (newPosition > data.length) {
                newPosition = data.length;
            }
            final int toReadBytes = newPosition - position;
            System.arraycopy(data, position, bytes, 0, toReadBytes);
            position = newPosition;
            return toReadBytes;
        } else {
            return -1;
        }
    }

    protected int getDataLength() {
        return data.length;
    }

    protected void setPosition(final long position) {
        this.position = (int) position;
    }

    @Override
    public void skip(final long newPosition) {
        this.position = this.position + (int) newPosition;
    }
}
