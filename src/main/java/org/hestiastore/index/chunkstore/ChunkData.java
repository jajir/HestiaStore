package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

public class ChunkData {

    private final long flags;
    private final long crc;
    private final long magicNumber;
    private final int version;
    private final Bytes payload;

    private ChunkData(final long flags, final long crc, final long magicNumber,
            final int version, final Bytes payload) {
        this.flags = flags;
        this.crc = crc;
        this.magicNumber = magicNumber;
        this.version = version;
        this.payload = Vldtn.requireNonNull(payload, "payload");
    }

    public long getCrc() {
        return crc;
    }

    public long getFlags() {
        return flags;
    }

    public long getMagicNumber() {
        return magicNumber;
    }

    public int getVersion() {
        return version;
    }

    public Bytes getPayload() {
        return payload;
    }

}
