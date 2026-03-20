package org.hestiastore.index.bytes;

import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.hestiastore.index.Vldtn;

/**
 * CRC32 adapter that can consume {@link ByteSequence} instances without forcing
 * callers to convert manually.
 */
public final class ByteSequenceCrc32 implements Checksum {

    private final CRC32 delegate = new CRC32();

    @Override
    public void update(final int b) {
        delegate.update(b);
    }

    @Override
    public void update(final byte[] b, final int off, final int len) {
        final byte[] validated = Vldtn.requireNonNull(b, "bytes");
        if (off < 0 || len < 0 || off > validated.length
                || ((long) off + (long) len) > validated.length) {
            final long rangeEnd = (long) off + (long) len;
            throw new IllegalArgumentException(String.format(
                    "Range [%d, %d) exceeds array length %d", off, rangeEnd,
                    validated.length));
        }
        delegate.update(validated, off, len);
    }

    /**
     * Updates the CRC with the content of the provided {@link ByteSequence}.
     *
     * @param sequence the byte sequence to consume
     */
    public void update(final ByteSequence sequence) {
        updateInternal(Vldtn.requireNonNull(sequence, "sequence"));
    }

    private void updateInternal(final ByteSequence sequence) {
        final int length = sequence.length();
        if (length == 0) {
            return;
        }
        if (sequence instanceof ConcatenatedByteSequence concatenated) {
            updateInternal(concatenated.firstPart());
            updateInternal(concatenated.secondPart());
            return;
        }
        if (sequence instanceof ByteSequenceView) {
            final byte[] bytes = sequence.toByteArray();
            delegate.update(bytes, 0, bytes.length);
            return;
        }
        if (sequence instanceof ByteSequenceSlice slice) {
            delegate.update(slice.backingArray(), slice.backingOffset(),
                    length);
            return;
        }
        if (sequence instanceof MutableBytes mutable) {
            delegate.update(mutable.array(), 0, length);
            return;
        }
        for (int i = 0; i < length; i++) {
            delegate.update(sequence.getByte(i));
        }
    }

    @Override
    public long getValue() {
        return delegate.getValue();
    }

    @Override
    public void reset() {
        delegate.reset();
    }
}
