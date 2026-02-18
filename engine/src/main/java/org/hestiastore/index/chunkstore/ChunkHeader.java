package org.hestiastore.index.chunkstore;

import java.util.Objects;
import java.util.Optional;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

/**
 * if (bytes == null) { return Optional.empty(); } return
 * ChunkHeaderCodec.decodeOptional(bytes.getData());
 * <table>
 * <tr>
 * <th>Field</th>
 * <th>Type</th>
 * <th>Starts at (byte)</th>
 * <th>Size (bytes)</th>
 * </tr>
 * 
 * 
 * <tr>
 * <td>Magic Number</td>
 * <td>long</td>
 * <td>0</td>
 * <td>8</td>
 * </tr>
 * 
 * 
 * <tr>
 * <td>Header version</td>
 * <td>int</td>
 * <td>8</td>
 * <td>4</td>
 * </tr>
 * 
 * 
 * <tr>
 * <td>Payload data length</td>
 * <td>int</td>
 * <td>12</td>
 * <td>4</td>
 * </tr>
 * 
 * 
 * <tr>
 * <td>CRC32 payload code</td>
 * <td>long</td>
 * <td>16</td>
 * <td>8</td>
 * </tr>
 * 
 * 
 * <tr>
 * <td>Flags for further usage. Flags closely describes the chunk content.</td>
 * <td>long</td>
 * <td>24</td>
 * <td>8</td>
 * </tr>
 * 
 * 
 * </table>
 * <ul>
 * Header will be always 32 bytes long.
 */
public final class ChunkHeader {

    /**
     * Size of the chunk header in bytes.
     */
    static final int HEADER_SIZE = 32;

    /**
     * "theodora" in ASCII
     */
    public static final long MAGIC_NUMBER = 0x7468656F646F7261L;

    private final long magicNumber;
    private final int version;
    private final int payloadLength;
    private final long crc;
    private final long flags;

    /**
     * Creates a chunk header from the given byte array.
     * 
     * @param data required byte array, must be exactly 32 bytes long
     * @return the chunk header
     */
    public static ChunkHeader of(final byte[] data) {
        return ChunkHeaderCodec.decode(data);
    }

    /**
     * Creates a chunk header from the given bytes.
     * 
     * @param bytes required bytes, must be exactly 32 bytes long
     * @return the chunk header
     */
    public static ChunkHeader of(final Bytes bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        return ChunkHeaderCodec.decode(bytes.getData());
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
        return ChunkHeaderCodec.decodeOptional(bytes.getData());
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
            final int payloadLength, final long crc) {
        return of(magic, version, payloadLength, crc, 0L);
    }

    /**
     * Creates a chunk header from the given parameters, including flags.
     *
     * @param magic         required magic number, must be {@link #MAGIC_NUMBER}
     * @param version       required version, must be positive
     * @param payloadLength required payload length, must be positive
     * @param crc           required CRC32 code, must be positive
     * @param flags         optional flags describing the payload, must be
     *                      non-negative
     * @return the chunk header
     */
    public static ChunkHeader of(final long magic, final int version,
            final int payloadLength, final long crc, final long flags) {
        return new ChunkHeader(magic, version, payloadLength, crc, flags);
    }

    ChunkHeader(final long magicNumber, final int version,
            final int payloadLength, final long crc, final long flags) {
        this.magicNumber = validateMagic(magicNumber);
        this.version = version;
        this.payloadLength = validatePayloadLength(payloadLength);
        this.crc = crc;
        this.flags = flags;
    }

    private static long validateMagic(final long magic) {
        if (magic != MAGIC_NUMBER) {
            throw new IllegalArgumentException(String.format(
                    "Invalid chunk magic number '%s', expected '%s'", magic,
                    MAGIC_NUMBER));
        }
        return magic;
    }

    private static int validatePayloadLength(final int payloadLength) {
        return Vldtn.requireGreaterThanZero(payloadLength, "payloadLength");
    }

    /**
     * Returns the byte array representing the chunk header.
     * 
     * @return the byte array representing the chunk header
     */
    public Bytes getBytes() {
        return Bytes.of(ChunkHeaderCodec.encode(this));
    }

    /**
     * Returns the magic number.
     * 
     * @return the magic number
     */
    public long getMagicNumber() {
        return magicNumber;
    }

    /**
     * Returns the version.
     * 
     * @return the version
     */
    public int getVersion() {
        return version;
    }

    /**
     * Returns the payload length.
     * 
     * @return the payload length
     */
    public int getPayloadLength() {
        return payloadLength;
    }

    /**
     * Returns the CRC32 code.
     * 
     * @return the CRC32 code
     */
    public long getCrc() {
        return crc;
    }

    /**
     * Returns the chunk flags.
     * 
     * @return the chunk flags
     */
    public long getFlags() {
        return flags;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof ChunkHeader))
            return false;
        final ChunkHeader that = (ChunkHeader) o;
        return magicNumber == that.magicNumber && version == that.version
                && payloadLength == that.payloadLength && crc == that.crc
                && flags == that.flags;
    }

    @Override
    public int hashCode() {
        return Objects.hash(magicNumber, version, payloadLength, crc, flags);
    }

    @Override
    public String toString() {
        return "ChunkHeader{" + "magic=" + getMagicNumber() + ", version="
                + getVersion() + ", payloadLength=" + getPayloadLength()
                + ", crc=" + getCrc() + ", flags=" + getFlags() + '}';
    }

}
