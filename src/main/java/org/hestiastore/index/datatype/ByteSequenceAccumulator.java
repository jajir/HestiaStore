package org.hestiastore.index.datatype;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequenceView;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

/**
 * In-memory {@link FileWriter} that buffers written bytes inside a growable
 * list of {@link ByteSequence} segments. Callers can append single bytes or
 * {@link ByteSequence} instances and later retrieve the aggregated payload as
 * either a raw {@code byte[]} or immutable {@link ByteSequenceView} snapshot.
 * Close the writer to release resources associated with the stored segments.
 */
public class ByteSequenceAccumulator extends AbstractCloseableResource
        implements FileWriter {

    public static ByteSequenceAccumulator create() {
        return new ByteSequenceAccumulator();
    }

    private final List<ByteSequence> segments;
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
        segments.add(ByteSequences.wrap(new byte[] { b }));
        totalLength += 1;
    }

    @Override
    public void write(final ByteSequence bytes) {
        final ByteSequence checked = Vldtn.requireNonNull(bytes, "bytes");
        if (checked.isEmpty()) {
            return;
        }
        segments.add(checked);
        totalLength += checked.length();
    }

    /**
     * Returns a copy of the bytes written to the buffer so far.
     *
     * @return accumulated data as a new {@code byte[]} instance
     */
    byte[] toByteArray() {
        final byte[] out = new byte[totalLength];
        int offset = 0;
        for (ByteSequence segment : segments) {
            final int length = segment.length();
            if (length == 0) {
                continue;
            }
            segment.copyTo(0, out, offset, length);
            offset += length;
        }
        return out;
    }

    /**
     * Returns the buffered data as an immutable {@link ByteSequenceView}
     * snapshot.
     *
     * @return immutable representation of the written bytes
     */
    public ByteSequence toBytes() {
        return ByteSequences.wrap(toByteArray());
    }

}
