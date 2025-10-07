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

    public static ChunkData of(final long flags, final long crc,
            final long magicNumber, final int version, final Bytes payload) {
        return new ChunkData(flags, crc, magicNumber, version, payload);
    }

    public ChunkData withFlags(final long newFlags) {
        return new ChunkData(newFlags, crc, magicNumber, version, payload);
    }

    public ChunkData withCrc(final long newCrc) {
        return new ChunkData(flags, newCrc, magicNumber, version, payload);
    }

    public ChunkData withMagicNumber(final long newMagicNumber) {
        return new ChunkData(flags, crc, newMagicNumber, version, payload);
    }

    public ChunkData withVersion(final int newVersion) {
        return new ChunkData(flags, crc, magicNumber, newVersion, payload);
    }

    public ChunkData withPayload(final Bytes newPayload) {
        return new ChunkData(flags, crc, magicNumber, version,
                Vldtn.requireNonNull(newPayload, "payload"));
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
