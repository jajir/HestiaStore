package org.hestiastore.index.chunkstore;

import org.apache.commons.codec.digest.PureJavaCrc32;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

public class ChunkPayload {

    private final Bytes bytes;

    public static ChunkPayload of(final Bytes bytes) {
        return new ChunkPayload(bytes);
    }

    public ChunkPayload(final Bytes bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        this.bytes = bytes;
    }

    public Bytes getBytes() {
        return bytes;
    }

    public int length() {
        return bytes.length();
    }

    public long calculateCrc() {
        final PureJavaCrc32 crc = new PureJavaCrc32();
        crc.update(bytes.getData());
        return crc.getValue();
    }
}
