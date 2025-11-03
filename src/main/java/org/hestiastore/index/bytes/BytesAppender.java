package org.hestiastore.index.bytes;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Vldtn;

/**
 * Allows appending multiple byte sequences without repeated copying.
 */
public class BytesAppender {

    private final List<ByteSequence> bytes = new ArrayList<>();

    /**
     * Append the provided sequence to the accumulator.
     *
     * @param data sequence to append (must not be {@code null})
     */
    public void append(final ByteSequence data) {
        final ByteSequence validated = Vldtn.requireNonNull(data, "bytes");
        if (validated.length() == 0) {
            return; // skip empty sequences
        }
        bytes.add(validated);
    }

    /**
     * Concatenate and return all appended sequences as a single
     * {@link ByteSequence}.
     *
     * @return combined sequence
     */
    public ByteSequence getBytes() {
        int length = 0;
        for (ByteSequence sequence : bytes) {
            length += sequence.length();
        }
        final byte[] combined = new byte[length];
        int offset = 0;
        for (ByteSequence sequence : bytes) {
            final int chunkLength = sequence.length();
            ByteSequences.copy(sequence, 0, combined, offset, chunkLength);
            offset += chunkLength;
        }
        return ByteSequences.wrap(combined);
    }
}
