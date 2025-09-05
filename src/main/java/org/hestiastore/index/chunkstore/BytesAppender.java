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

    public void append(final byte[] data) {
        Vldtn.requireNonNull(data, "data");
        if (data.length == 0) {
            return; // No need to append empty byte arrays
        }
        bytes.add(Bytes.of(data));
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
