package org.hestiastore.index.chunkstore;

import java.util.ArrayList;
import java.util.List;

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
    public void append(final Bytes data) {
        final Bytes validated = Vldtn.requireNonNull(data, "bytes");
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
    public Bytes getBytes() {
        int length = 0;
        for (Bytes b : bytes) {
            length += b.length();
        }
        final byte[] combined = new byte[length];
        int offset = 0;
        for (Bytes b : bytes) {
            System.arraycopy(b.getData(), 0, combined, offset, b.length());
            offset += b.length();
        }
        return Bytes.of(combined);
    }

}
