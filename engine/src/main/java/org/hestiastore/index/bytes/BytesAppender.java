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
        if (parts.isEmpty()) {
            return ByteSequence.EMPTY;
        }
        return ByteSequences.concatNonEmpty(parts);
    }
}
