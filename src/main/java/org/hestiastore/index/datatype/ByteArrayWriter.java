package org.hestiastore.index.datatype;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

/**
 * In-memory {@link FileWriter} that buffers written bytes inside a
 * {@link ByteArrayOutputStream}. Callers can append single bytes or complete
 * {@link ByteSequence} instances and later retrieve the aggregated payload as
 * either a raw {@code byte[]} or immutable {@link Bytes} snapshot. Close the
 * writer to release resources associated with the underlying stream.
 */
public class ByteArrayWriter extends AbstractCloseableResource
        implements FileWriter {

    private final ByteArrayOutputStream fio;

    ByteArrayWriter() {
        this.fio = new ByteArrayOutputStream();
    }

    @Override
    protected void doClose() {
        try {
            fio.close();
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public void write(byte b) {
        fio.write(b);
    }

    @Override
    public void write(final ByteSequence bytes) {
        final byte[] data = Vldtn.requireNonNull(bytes, "bytes").toByteArray();
        try {
            fio.write(data);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    /**
     * Returns a copy of the bytes written to the buffer so far.
     *
     * @return accumulated data as a new {@code byte[]} instance
     */
    byte[] toByteArray() {
        return fio.toByteArray();
    }

    /**
     * Returns the buffered data as an immutable {@link Bytes} snapshot.
     *
     * @return immutable representation of the written bytes
     */
    Bytes toBytes() {
        return Bytes.of(fio.toByteArray());
    }

}
