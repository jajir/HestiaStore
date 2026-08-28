package org.hestiastore.index.chunkentryfile;

import java.util.Arrays;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.directory.FileWriter;

/**
 * Growable in-memory writer backed by one contiguous byte buffer.
 *
 * <p>
 * The buffer is reused for every field written to a chunk and grows only when
 * the encoded page requires it. This avoids retaining one object and one byte
 * array for every small serializer write. Capacity is explicitly capped at the
 * largest safe Java array size.
 * </p>
 */
class InMemoryFileWriter extends AbstractCloseableResource
        implements FileWriter {
    private static final int INITIAL_CAPACITY = 8 * 1024;
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private final int maxCapacity;
    private byte[] buffer;
    private int position;
    private ByteSequence encoded;
    private boolean closed = false;

    /**
     * Creates a writer using the maximum safe Java array size as its cap.
     */
    InMemoryFileWriter() {
        this(MAX_ARRAY_SIZE);
    }

    /**
     * Creates a writer whose buffer cannot grow past the given capacity.
     *
     * @param maxCapacity positive maximum encoded byte count
     */
    InMemoryFileWriter(final int maxCapacity) {
        this.maxCapacity = Vldtn.requireBetween(maxCapacity, 1,
                MAX_ARRAY_SIZE, "maxCapacity");
        buffer = new byte[Math.min(INITIAL_CAPACITY, maxCapacity)];
    }

    @Override
    public void write(final byte b) {
        ensureOpen();
        ensureCapacity(1);
        buffer[position++] = b;
    }

    @Override
    public void write(final byte[] bytes) {
        write(bytes, 0, Vldtn.requireNonNull(bytes, "bytes").length);
    }

    @Override
    public void write(final byte[] bytes, final int offset, final int length) {
        final byte[] validated = Vldtn.requireNonNull(bytes, "bytes");
        final int from = Vldtn.requireGreaterThanOrEqualToZero(offset,
                "offset");
        final int len = Vldtn.requireGreaterThanOrEqualToZero(length, "length");
        if (from > validated.length
                || (long) from + len > validated.length) {
            throw new IllegalArgumentException(String.format(
                    "Offset '%s' and length '%s' exceed source length '%s'",
                    from, len, validated.length));
        }
        ensureOpen();
        if (len == 0) {
            return;
        }
        ensureCapacity(len);
        System.arraycopy(validated, from, buffer, position, len);
        position += len;
    }

    /**
     * Closes this writer and returns the encoded bytes as one flat sequence.
     *
     * @return immutable encoded byte sequence
     */
    ByteSequence closeSequence() {
        close();
        if (encoded != null) {
            return encoded;
        }
        if (position == 0) {
            encoded = ByteSequence.EMPTY;
            return encoded;
        }
        if (position == buffer.length) {
            encoded = ByteSequences.wrap(buffer);
            return encoded;
        }
        encoded = ByteSequences.wrap(Arrays.copyOf(buffer, position));
        return encoded;
    }

    @Override
    protected void doClose() {
        closed = true;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("FileWriter already closed");
        }
    }

    private void ensureCapacity(final int additionalBytes) {
        final long required = (long) position + additionalBytes;
        if (required > maxCapacity) {
            throw new IllegalStateException(
                    "Encoded chunk exceeds its bounded in-memory byte buffer");
        }
        if (required <= buffer.length) {
            return;
        }
        final long grown = (long) buffer.length + (buffer.length >>> 1);
        final int newCapacity = Math.max((int) required,
                (int) Math.min(grown, maxCapacity));
        buffer = Arrays.copyOf(buffer, newCapacity);
    }
}
