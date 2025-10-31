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

    private final List<Bytes> bytes = new ArrayList<>();

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
        final Bytes toAdd = validated instanceof Bytes ? (Bytes) validated
                : Bytes.copyOf(validated);
        bytes.add(toAdd);
    }

    /**
     * Return concatenated all appended bytes.
     *
     * @return
     */
    public ByteSequence getBytes() {
        int length = 0;
        for (Bytes b : bytes) {
            length += b.length();
        }
        final byte[] combined = new byte[length];
        int offset = 0;
        for (Bytes b : bytes) {
            b.copyTo(0, combined, offset, b.length());
            offset += b.length();
        }
        return Bytes.of(combined);
    }

}
