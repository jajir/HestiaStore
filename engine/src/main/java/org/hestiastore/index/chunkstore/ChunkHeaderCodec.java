package org.hestiastore.index.chunkstore;

import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;

/**
 * Provides encoding and decoding utilities for {@link ChunkHeader}.
 */
final class ChunkHeaderCodec {

    private static final int MAGIC_OFFSET = 0;
    private static final int VERSION_OFFSET = 8;
    private static final int PAYLOAD_LENGTH_OFFSET = 12;
    private static final int CRC_OFFSET = 16;
    private static final int FLAGS_OFFSET = 24;

    private ChunkHeaderCodec() {
        // utility class
    }

    static ChunkHeader decode(final byte[] data) {
        Vldtn.requireNonNull(data, "data");
        if (data.length != ChunkHeader.HEADER_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "Invalid chunk header size '%d', expected '%d'",
                    data.length, ChunkHeader.HEADER_SIZE));
        }

        final long magic = readLong(data, MAGIC_OFFSET);
        final int version = readInt(data, VERSION_OFFSET);
        final int payloadLength = readInt(data, PAYLOAD_LENGTH_OFFSET);
        final long crc = readLong(data, CRC_OFFSET);
        final long flags = readLong(data, FLAGS_OFFSET);

        return new ChunkHeader(magic, version, payloadLength, crc, flags);
    }

    static ChunkHeader decode(final ByteSequence data) {
        Vldtn.requireNonNull(data, "data");
        if (data.length() != ChunkHeader.HEADER_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "Invalid chunk header size '%d', expected '%d'",
                    data.length(), ChunkHeader.HEADER_SIZE));
        }

        final long magic = readLong(data, MAGIC_OFFSET);
        final int version = readInt(data, VERSION_OFFSET);
        final int payloadLength = readInt(data, PAYLOAD_LENGTH_OFFSET);
        final long crc = readLong(data, CRC_OFFSET);
        final long flags = readLong(data, FLAGS_OFFSET);

        return new ChunkHeader(magic, version, payloadLength, crc, flags);
    }

    static Optional<ChunkHeader> decodeOptional(final byte[] data) {
        if (data == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(decode(data));
        } catch (IllegalArgumentException | NullPointerException ex) {
            return Optional.empty();
        }
    }

    static Optional<ChunkHeader> decodeOptional(final ByteSequence data) {
        if (data == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(decode(data));
        } catch (IllegalArgumentException | NullPointerException ex) {
            return Optional.empty();
        }
    }

    static byte[] encode(final ChunkHeader header) {
        final byte[] data = new byte[ChunkHeader.HEADER_SIZE];
        writeLong(data, MAGIC_OFFSET, header.getMagicNumber());
        writeInt(data, VERSION_OFFSET, header.getVersion());
        writeInt(data, PAYLOAD_LENGTH_OFFSET, header.getPayloadLength());
        writeLong(data, CRC_OFFSET, header.getCrc());
        writeLong(data, FLAGS_OFFSET, header.getFlags());
        return data;
    }

    private static long readLong(final byte[] data, final int offset) {
        return (data[offset] & 0xFFL) << 56
                | (data[offset + 1] & 0xFFL) << 48
                | (data[offset + 2] & 0xFFL) << 40
                | (data[offset + 3] & 0xFFL) << 32
                | (data[offset + 4] & 0xFFL) << 24
                | (data[offset + 5] & 0xFFL) << 16
                | (data[offset + 6] & 0xFFL) << 8
                | (data[offset + 7] & 0xFFL);
    }

    private static int readInt(final byte[] data, final int offset) {
        return (data[offset] & 0xFF) << 24
                | (data[offset + 1] & 0xFF) << 16
                | (data[offset + 2] & 0xFF) << 8
                | (data[offset + 3] & 0xFF);
    }

    private static long readLong(final ByteSequence data, final int offset) {
        return (data.getByte(offset) & 0xFFL) << 56
                | (data.getByte(offset + 1) & 0xFFL) << 48
                | (data.getByte(offset + 2) & 0xFFL) << 40
                | (data.getByte(offset + 3) & 0xFFL) << 32
                | (data.getByte(offset + 4) & 0xFFL) << 24
                | (data.getByte(offset + 5) & 0xFFL) << 16
                | (data.getByte(offset + 6) & 0xFFL) << 8
                | (data.getByte(offset + 7) & 0xFFL);
    }

    private static int readInt(final ByteSequence data, final int offset) {
        return (data.getByte(offset) & 0xFF) << 24
                | (data.getByte(offset + 1) & 0xFF) << 16
                | (data.getByte(offset + 2) & 0xFF) << 8
                | (data.getByte(offset + 3) & 0xFF);
    }

    private static void writeLong(final byte[] data, final int offset,
            final long value) {
        data[offset] = (byte) (value >>> 56);
        data[offset + 1] = (byte) (value >>> 48);
        data[offset + 2] = (byte) (value >>> 40);
        data[offset + 3] = (byte) (value >>> 32);
        data[offset + 4] = (byte) (value >>> 24);
        data[offset + 5] = (byte) (value >>> 16);
        data[offset + 6] = (byte) (value >>> 8);
        data[offset + 7] = (byte) value;
    }

    private static void writeInt(final byte[] data, final int offset,
            final int value) {
        data[offset] = (byte) (value >>> 24);
        data[offset + 1] = (byte) (value >>> 16);
        data[offset + 2] = (byte) (value >>> 8);
        data[offset + 3] = (byte) value;
    }
}
