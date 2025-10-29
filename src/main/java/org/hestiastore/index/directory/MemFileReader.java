package org.hestiastore.index.directory;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.MutableByteSequence;
import org.hestiastore.index.Vldtn;

public class MemFileReader extends AbstractCloseableResource
        implements FileReader {

    private final ByteSequence source;

    private int position;

    public MemFileReader(final ByteSequence bytes) {
        this.source = Bytes.copyOf(Vldtn.requireNonNull(bytes, "bytes"));
        position = 0;
    }

    private void requireOpen() {
        if (position < 0) {
            throw new IllegalStateException("MemFileReader already closed");
        }
    }

    @Override
    protected void doClose() {
        position = -1;
    }

    @Override
    public int read() {
        requireOpen();
        if (position < source.length()) {
            return source.getByte(position++) & 0xFF;
        } else {
            return -1;
        }
    }

    @Override
    public int read(final MutableByteSequence bytes) {
        requireOpen();
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

    protected void setPosition(final long targetPosition) {
        requireOpen();
        if (targetPosition < 0) {
            throw new IllegalArgumentException(
                    String.format("Position '%s' is invalid", targetPosition));
        }
        if (targetPosition > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(String.format(
                    "Position '%s' exceeds supported range", targetPosition));
        }
        if (targetPosition > source.length()) {
            throw new IllegalArgumentException(
                    String.format("Position '%s' exceeds data length '%s'",
                            targetPosition, source.length()));
        }
        this.position = (int) targetPosition;
    }

    @Override
    public void skip(final long newPosition) {
        requireOpen();
        if (newPosition < 0) {
            throw new IllegalArgumentException(String
                    .format("Bytes to skip '%s' is invalid", newPosition));
        }
        final long available = (long) source.length() - position;
        if (newPosition > available) {
            throw new IllegalArgumentException(String.format(
                    "Bytes to skip '%s' exceeds available data '%s'",
                    newPosition, available));
        }
        this.position += (int) newPosition;
    }
}
