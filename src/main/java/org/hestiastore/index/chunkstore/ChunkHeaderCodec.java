package org.hestiastore.index.chunkstore;

import java.util.Optional;

import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.MutableBytes;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ConvertorFromBytes;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;

/**
 * Provides encoding and decoding utilities for {@link ChunkHeader}.
 */
final class ChunkHeaderCodec {

    private static final int MAGIC_OFFSET = 0;
    private static final int VERSION_OFFSET = 8;
    private static final int PAYLOAD_LENGTH_OFFSET = 12;
    private static final int CRC_OFFSET = 16;
    private static final int FLAGS_OFFSET = 24;

    private static final TypeDescriptor<Long> TYPE_DESCRIPTOR_LONG = new TypeDescriptorLong();
    private static final ConvertorFromBytes<Long> LONG_FROM_BYTES = TYPE_DESCRIPTOR_LONG
            .getConvertorFromBytes();
    private static final ConvertorToBytes<Long> LONG_TO_BYTES = TYPE_DESCRIPTOR_LONG
            .getConvertorToBytes();

    private static final TypeDescriptor<Integer> TYPE_DESCRIPTOR_INTEGER = new TypeDescriptorInteger();
    private static final ConvertorFromBytes<Integer> INT_FROM_BYTES = TYPE_DESCRIPTOR_INTEGER
            .getConvertorFromBytes();
    private static final ConvertorToBytes<Integer> INT_TO_BYTES = TYPE_DESCRIPTOR_INTEGER
            .getConvertorToBytes();

    private ChunkHeaderCodec() {
        // utility class
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

    static ByteSequence encode(final ChunkHeader header) {
        final MutableBytes out = MutableBytes.allocate(ChunkHeader.HEADER_SIZE);
        writeLong(out, MAGIC_OFFSET, header.getMagicNumber());
        writeInt(out, VERSION_OFFSET, header.getVersion());
        writeInt(out, PAYLOAD_LENGTH_OFFSET, header.getPayloadLength());
        writeLong(out, CRC_OFFSET, header.getCrc());
        writeLong(out, FLAGS_OFFSET, header.getFlags());
        return out.toBytes();
    }

    private static long readLong(final ByteSequence data, final int offset) {
        final ByteSequence buffer = data.slice(offset, offset + 8);
        final Long value = LONG_FROM_BYTES.fromBytes(buffer);
        if (value == null) {
            throw new IllegalArgumentException(
                    "Unable to read long from chunk header");
        }
        return value;
    }

    private static int readInt(final ByteSequence data, final int offset) {
        final ByteSequence buffer = data.slice(offset, offset + 4);
        final Integer value = INT_FROM_BYTES.fromBytes(buffer);
        if (value == null) {
            throw new IllegalArgumentException(
                    "Unable to read int from chunk header");
        }
        return value;
    }

    private static void writeLong(final MutableBytes data, final int offset,
            final long value) {
        final ByteSequence bytes = LONG_TO_BYTES.toBytesBuffer(value);
        data.setBytes(offset, bytes);
    }

    private static void writeInt(final MutableBytes data, final int offset,
            final int value) {
        final ByteSequence bytes = INT_TO_BYTES.toBytesBuffer(value);
        data.setBytes(offset, bytes);
    }
}
