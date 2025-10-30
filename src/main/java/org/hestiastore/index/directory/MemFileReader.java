package org.hestiastore.index.directory;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.MutableBytes;
import org.hestiastore.index.Vldtn;

public class MemFileReader extends AbstractCloseableResource
        implements FileReader {

    private final ByteSequence source;

    private int position;

    public MemFileReader(final ByteSequence bytes) {
        this.source = Vldtn.requireNonNull(bytes, "bytes");
        position = 0;
    }

    @Override
    protected void doClose() {
        position = -1;
    }

    @Override
    public int read() {
        if (position < source.length()) {
            return source.getByte(position++) & 0xFF;
        } else {
            return -1;
        }
    }

    @Override
    public int read(final MutableBytes bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        if (position < source.length()) {
            int newPosition = position + bytes.length();
            if (newPosition > source.length()) {
                newPosition = source.length();
            }
            final int toReadBytes = newPosition - position;
            bytes.setBytes(0, source, position, toReadBytes);
            position = newPosition;
            return toReadBytes;
        } else {
            return -1;
        }
    }

    protected int getDataLength() {
        return source.length();
    }

    protected void setPosition(final long position) {
        this.position = (int) position;
    }

    @Override
    public void skip(final long newPosition) {
        this.position = this.position + (int) newPosition;
    }
}
