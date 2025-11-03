package org.hestiastore.index.chunkstore;

import java.util.Optional;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequenceView;
import org.hestiastore.index.bytes.MutableBytes;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockByteReader;

public class ChunkData {

    private final long flags;
    private final long crc;
    private final long magicNumber;
    private final int version;
    private final ByteSequence payload;

    private ChunkData(final long flags, final long crc, final long magicNumber,
            final int version, final ByteSequence payload) {
        this.flags = flags;
        this.crc = crc;
        this.magicNumber = magicNumber;
        this.version = version;
        this.payload = normalizePayload(payload);
    }

    public static ChunkData of(final long flags, final long crc,
            final long magicNumber, final int version,
            final ByteSequence payload) {
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

    public ChunkData withPayload(final ByteSequence newPayload) {
        return new ChunkData(flags, crc, magicNumber, version, newPayload);
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

    public ByteSequence getPayload() {
        return payload;
    }

    static Optional<ChunkData> read(final DataBlockByteReader reader) {
        final ByteSequence headerSequence = reader
                .readExactly(ChunkHeader.HEADER_SIZE);
        if (headerSequence == null) {
            return Optional.empty();
        }
        final Optional<ChunkHeader> optionalChunkHeader = ChunkHeader
                .optionalOf(headerSequence);
        if (optionalChunkHeader.isEmpty()) {
            return Optional.empty();
        }
        final ChunkHeader chunkHeader = optionalChunkHeader.get();
        final int payloadLength = chunkHeader.getPayloadLength();
        final int cellLength = convertLengthToWholeCells(payloadLength);
        ByteSequence payload = reader.readExactly(cellLength);
        if (payload == null) {
            throw new IllegalStateException(
                    "Unexpected end of stream while reading chunk payload.");
        }
        if (cellLength != payloadLength) {
            payload = payload.slice(0, payloadLength);
        }
        return Optional.of(ChunkData.of(chunkHeader.getFlags(),
                chunkHeader.getCrc(), chunkHeader.getMagicNumber(),
                chunkHeader.getVersion(), payload));
    }

    private static int convertLengthToWholeCells(final int length) {
        int out = length / CellPosition.CELL_SIZE;
        if (length % CellPosition.CELL_SIZE != 0) {
            out++;
        }
        return out * CellPosition.CELL_SIZE;
    }

    private static ByteSequence normalizePayload(final ByteSequence sequence) {
        final ByteSequence validated = Vldtn.requireNonNull(sequence,
                "payload");
        if (validated.isEmpty()) {
            return ByteSequence.EMPTY;
        }
        return validated;
    }
}
