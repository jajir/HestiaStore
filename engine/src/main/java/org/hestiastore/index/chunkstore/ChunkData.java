package org.hestiastore.index.chunkstore;

import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
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
        this.payload = Vldtn.requireNonNull(payload, "payload");
    }

    /**
     * Creates a chunk data instance with payload represented as
     * {@link ByteSequence}.
     *
     * @param flags       metadata flags
     * @param crc         crc32 checksum
     * @param magicNumber chunk magic number
     * @param version     chunk version
     * @param payload     payload sequence
     * @return chunk data instance
     */
    public static ChunkData ofSequence(final long flags, final long crc,
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

    /**
     * Returns a copy of this instance with replaced payload.
     *
     * @param newPayload new payload sequence
     * @return updated chunk data
     */
    public ChunkData withPayloadSequence(final ByteSequence newPayload) {
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

    /**
     * Returns chunk payload as byte sequence.
     *
     * @return payload sequence
     */
    public ByteSequence getPayloadSequence() {
        return payload;
    }

    static Optional<ChunkData> read(final DataBlockByteReader reader) {
        final ByteSequence headerBytes = reader
                .readExactlySequence(ChunkHeader.HEADER_SIZE);
        if (headerBytes == null) {
            return Optional.empty();
        }
        final Optional<ChunkHeader> optionalChunkHeader = ChunkHeader
                .optionalOfSequence(headerBytes);
        if (optionalChunkHeader.isEmpty()) {
            return Optional.empty();
        }
        final ChunkHeader chunkHeader = optionalChunkHeader.get();
        final int payloadLength = chunkHeader.getPayloadLength();
        final int cellLength = convertLengthToWholeCells(payloadLength);
        ByteSequence payload = reader.readExactlySequence(cellLength);
        if (payload == null) {
            throw new IllegalStateException(
                    "Unexpected end of stream while reading chunk payload.");
        }
        if (cellLength != payloadLength) {
            payload = payload.slice(0, payloadLength);
        }
        return Optional.of(ChunkData.ofSequence(chunkHeader.getFlags(),
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

}
