package org.hestiastore.index.bytes;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Vldtn;

/**
 * Allows appending multiple byte sequences without repeated copying.
 */
public final class BytesAppender {

    private final List<ByteSequence> parts = new ArrayList<>();

    /**
     * Append the provided sequence to the accumulator.
     *
     * @param data sequence to append (must not be {@code null})
     */
    public void append(final ByteSequence data) {
        final ByteSequence validated = Vldtn.requireNonNull(data, "data");
        if (validated.length() == 0) {
            return; // skip empty sequences
        }
        parts.add(validated);
    }

    /**
     * Concatenate and return all appended sequences as a single
     * {@link ByteSequence}.
     *
     * @return combined sequence
     */
    public ByteSequence getBytes() {
        int length = 0;
        for (ByteSequence sequence : parts) {
            length = Math.addExact(length, sequence.length());
        }
        if (length == 0) {
            return ByteSequence.EMPTY;
        }
        final byte[] combined = new byte[length];
        int offset = 0;
        for (ByteSequence sequence : parts) {
            final int chunkLength = sequence.length();
            ByteSequences.copy(sequence, 0, combined, offset, chunkLength);
            offset += chunkLength;
        }
        return ByteSequences.wrap(combined);
    }
}
