package org.hestiastore.index.chunkstore;

import java.util.Optional;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ConvertorFromBytes;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;

/**
 * Represents the header of a chunk. It will contains:
 * <ul>
 * <li>Magic number to identify the chunk format - long</li>
 * <li>Version of the chunk format - int</li>
 * <li>Payload data length - int</li>
 * <li>CRC32 code - long</li>
 * </ul>
 * 
 * Header will be always 32 bytes long.
 * 
 * Equals and hashCode are implemented based on the byte array content.
 */
public class ChunkHeader {

    private static final TypeDescriptor<Long> TYPE_DESCRIPTOR_LONG = new TypeDescriptorLong();
    private static final ConvertorFromBytes<Long> LONG_CONVERTOR_FROM_BYTES = TYPE_DESCRIPTOR_LONG
            .getConvertorFromBytes();
    private static final ConvertorToBytes<Long> LONG_CONVERTOR_TO_BYTES = TYPE_DESCRIPTOR_LONG
            .getConvertorToBytes();
    private static final TypeDescriptor<Integer> TYPE_DESCRIPTOR_INTEGER = new TypeDescriptorInteger();
    private static final ConvertorFromBytes<Integer> INTEGER_CONVERTOR_FROM_BYTES = TYPE_DESCRIPTOR_INTEGER
            .getConvertorFromBytes();
    private static final ConvertorToBytes<Integer> INTEGER_CONVERTOR_TO_BYTES = TYPE_DESCRIPTOR_INTEGER
            .getConvertorToBytes();

    /**
     * Size of the chunk header in bytes.
     */
    static final int HEADER_SIZE = 32;

    /**
     * "theodora" in ASCII
     */
    public static final long MAGIC_NUMBER = 0x7468656F646F7261L;

    private final byte[] data;

    /**
     * Creates a chunk header from the given byte array.
     * 
     * @param data required byte array, must be exactly 32 bytes long
     * @return the chunk header
     */
    public static ChunkHeader of(final byte[] data) {
        return new ChunkHeader(data);
    }

    /**
     * Creates a chunk header from the given bytes.
     * 
     * @param bytes required bytes, must be exactly 32 bytes long
     * @return the chunk header
     */
    public static ChunkHeader of(final Bytes bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        return new ChunkHeader(bytes.getData());
    }

    /**
     * Creates an optional chunk header from the given bytes.
     * 
     * @param bytes bytes to create the chunk header from, can be null
     * @return optional chunk header, empty if the given bytes are null, or not
     *         exactly 32 bytes long, or the magic number is invalid
     */
    public static Optional<ChunkHeader> optionalOf(final Bytes bytes) {
        if (bytes == null) {
            return Optional.empty();
        }
        final byte[] data = bytes.getData();
        if (data == null) {
            return Optional.empty();
        }
        if (data.length != HEADER_SIZE) {
            return Optional.empty();
        }
        final byte[] buff = new byte[8];
        System.arraycopy(data, 0, buff, 0, 8);
        Long magic = LONG_CONVERTOR_FROM_BYTES.fromBytes(buff);
        if (magic == null || !magic.equals(MAGIC_NUMBER)) {
            return Optional.empty();
        }
        return Optional.of(new ChunkHeader(data));
    }

    /**
     * Creates a chunk header from the given parameters.
     * 
     * @param magic         required magic number, must be {@link #MAGIC_NUMBER}
     * @param version       required version, must be positive
     * @param payloadLength required payload length, must be positive
     * @param crc           required CRC32 code, must be positive
     * @return the chunk header
     */
    public static ChunkHeader of(final long magic, final int version,
            final int payloadLength, final Long crc) {
        final byte[] data = new byte[HEADER_SIZE];
        System.arraycopy(LONG_CONVERTOR_TO_BYTES.toBytes(magic), 0, data, 0, 8);
        System.arraycopy(INTEGER_CONVERTOR_TO_BYTES.toBytes(version), 0, data,
                8, 4);
        System.arraycopy(INTEGER_CONVERTOR_TO_BYTES.toBytes(payloadLength), 0,
                data, 12, 4);
        System.arraycopy(LONG_CONVERTOR_TO_BYTES.toBytes(crc), 0, data, 16, 8);
        return new ChunkHeader(data);
    }

    private ChunkHeader(final byte[] data) {
        this.data = Vldtn.requireNonNull(data, "data");
        if (data.length != HEADER_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "Invalid chunk header size '%d', expected is '%d'",
                    data.length, HEADER_SIZE));
        }
        if (getMagicNumber() != MAGIC_NUMBER) {
            throw new IllegalArgumentException(String.format(
                    "Invalid chunk magic number '%d', expected is '%d'",
                    getMagicNumber(), MAGIC_NUMBER));
        }
    }

    /**
     * Returns the byte array representing the chunk header.
     * 
     * @return the byte array representing the chunk header
     */
    public Bytes getBytes() {
        return Bytes.of(data);
    }

    /**
     * Returns the magic number.
     * 
     * @return the magic number
     */
    public long getMagicNumber() {
        final byte[] buff = new byte[8];
        System.arraycopy(data, 0, buff, 0, 8);
        return LONG_CONVERTOR_FROM_BYTES.fromBytes(buff);
    }

    /**
     * Returns the version.
     * 
     * @return the version
     */
    public int getVersion() {
        final byte[] buff = new byte[4];
        System.arraycopy(data, 8, buff, 0, 4);
        return INTEGER_CONVERTOR_FROM_BYTES.fromBytes(buff);
    }

    /**
     * Returns the payload length.
     * 
     * @return the payload length
     */
    public int getPayloadLength() {
        final byte[] buff = new byte[4];
        System.arraycopy(data, 12, buff, 0, 4);
        return INTEGER_CONVERTOR_FROM_BYTES.fromBytes(buff);
    }

    /**
     * Returns the CRC32 code.
     * 
     * @return the CRC32 code
     */
    public long getCrc() {
        final byte[] buff = new byte[8];
        System.arraycopy(data, 16, buff, 0, 8);
        return LONG_CONVERTOR_FROM_BYTES.fromBytes(buff);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof ChunkHeader))
            return false;
        final ChunkHeader that = (ChunkHeader) o;
        return java.util.Arrays.equals(this.data, that.data);
    }

    @Override
    public int hashCode() {
        return java.util.Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return "ChunkHeader{" + "magic=" + getMagicNumber() + ", version="
                + getVersion() + ", payloadLength=" + getPayloadLength()
                + ", crc=" + getCrc() + '}';
    }

}
