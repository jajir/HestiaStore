package org.hestiastore.index.directory;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

public class MemFileReader extends AbstractCloseableResource
        implements FileReader {

    private final Bytes source;
    private final byte[] data;

    private int position;

    public MemFileReader(final Bytes bytes) {
        this.source = Vldtn.requireNonNull(bytes, "bytes");
        this.data = source.getData();
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
    public int read(final Bytes bytes) {
        final byte[] target = Vldtn.requireNonNull(bytes, "bytes").getData();
        if (position < data.length) {
            int newPosition = position + target.length;
            if (newPosition > data.length) {
                newPosition = data.length;
            }
            final int toReadBytes = newPosition - position;
            System.arraycopy(data, position, target, 0, toReadBytes);
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
