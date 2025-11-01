package org.hestiastore.index.chunkstore;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

/**
 * Class allows to continuosly add bytes without periodic array copy calling.
 */
public class BytesAppender {

    private final List<ByteSequence> bytes = new ArrayList<>();

    /**
     * Append given bytes.
     *
     * @param data required data to append
     */
    public void append(final ByteSequence data) {
        final ByteSequence validated = Vldtn.requireNonNull(data, "bytes");
        if (validated.length() == 0) {
            return; // No need to append empty byte arrays
        }
        bytes.add(validated);
    }

    /**
     * Return concatenated all appended bytes.
     *
     * @return
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
            sequence.copyTo(0, combined, offset, chunkLength);
            offset += chunkLength;
        }
        return Bytes.of(combined);
    }

}
