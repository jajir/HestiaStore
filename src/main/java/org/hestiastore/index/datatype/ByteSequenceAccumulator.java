package org.hestiastore.index.datatype;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

/**
 * In-memory {@link FileWriter} that buffers written bytes inside a growable
 * list of {@link Bytes} segments. Callers can append single bytes or
 * {@link ByteSequence} instances and later retrieve the aggregated payload as
 * either a raw {@code byte[]} or immutable {@link Bytes} snapshot. Close the
 * writer to release resources associated with the stored segments.
 */
public class ByteSequenceAccumulator extends AbstractCloseableResource
        implements FileWriter {

    private final List<Bytes> segments;
    private int totalLength;

    ByteSequenceAccumulator() {
        this.segments = new ArrayList<>();
        this.totalLength = 0;
    }

    @Override
    protected void doClose() {
        segments.clear();
        totalLength = 0;
    }

    @Override
    public void write(byte b) {
        segments.add(Bytes.of(new byte[] { b }));
        totalLength += 1;
    }

    @Override
    public void write(final ByteSequence bytes) {
        final ByteSequence checked = Vldtn.requireNonNull(bytes, "bytes");
        // FIXME don't make defensive copy
        final Bytes segment = Bytes.copyOf(checked);
        if (segment.isEmpty()) {
            return;
        }
        segments.add(segment);
        totalLength += segment.length();
    }

    /**
     * Returns a copy of the bytes written to the buffer so far.
     *
     * @return accumulated data as a new {@code byte[]} instance
     */
    byte[] toByteArray() {
        final byte[] out = new byte[totalLength];
        int offset = 0;
        for (Bytes segment : segments) {
            final byte[] data = segment.toByteArray();
            System.arraycopy(data, 0, out, offset, data.length);
            offset += data.length;
        }
        return out;
    }

    /**
     * Returns the buffered data as an immutable {@link Bytes} snapshot.
     *
     * @return immutable representation of the written bytes
     */
    Bytes toBytes() {
        return Bytes.of(toByteArray());
    }

}
