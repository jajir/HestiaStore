package org.hestiastore.index.chunkstore;

import java.util.Optional;

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

    static ChunkHeader decode(final org.hestiastore.index.Bytes data) {
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

    static Optional<ChunkHeader> decodeOptional(
            final org.hestiastore.index.Bytes data) {
        if (data == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(decode(data));
        } catch (IllegalArgumentException | NullPointerException ex) {
            return Optional.empty();
        }
    }

    static org.hestiastore.index.Bytes encode(final ChunkHeader header) {
        final org.hestiastore.index.Bytes out = org.hestiastore.index.Bytes
                .allocate(ChunkHeader.HEADER_SIZE);
        writeLong(out, MAGIC_OFFSET, header.getMagicNumber());
        writeInt(out, VERSION_OFFSET, header.getVersion());
        writeInt(out, PAYLOAD_LENGTH_OFFSET, header.getPayloadLength());
        writeLong(out, CRC_OFFSET, header.getCrc());
        writeLong(out, FLAGS_OFFSET, header.getFlags());
        return out;
    }

    private static long readLong(final org.hestiastore.index.Bytes data,
            final int offset) {
        final org.hestiastore.index.Bytes buffer = data.subBytes(offset,
                offset + 8);
        final Long value = LONG_FROM_BYTES.fromBytes(buffer);
        if (value == null) {
            throw new IllegalArgumentException(
                    "Unable to read long from chunk header");
        }
        return value;
    }

    private static int readInt(final org.hestiastore.index.Bytes data,
            final int offset) {
        final org.hestiastore.index.Bytes buffer = data.subBytes(offset,
                offset + 4);
        final Integer value = INT_FROM_BYTES.fromBytes(buffer);
        if (value == null) {
            throw new IllegalArgumentException(
                    "Unable to read int from chunk header");
        }
        return value;
    }

    private static void writeLong(final org.hestiastore.index.Bytes data,
            final int offset, final long value) {
        final org.hestiastore.index.Bytes bytes = LONG_TO_BYTES
                .toBytesBuffer(value);
        System.arraycopy(bytes.getData(), 0, data.getData(), offset, 8);
    }

    private static void writeInt(final org.hestiastore.index.Bytes data,
            final int offset, final int value) {
        final org.hestiastore.index.Bytes bytes = INT_TO_BYTES
                .toBytesBuffer(value);
        System.arraycopy(bytes.getData(), 0, data.getData(), offset, 4);
    }
}
