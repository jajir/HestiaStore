package org.hestiastore.index.bytes;

import java.util.zip.Checksum;

import org.hestiastore.index.Vldtn;

/**
 * CRC32 implementation derived from Apache Commons' {@code PureJavaCrc32} that
 * can consume {@link ByteSequence} instances without first materialising a
 * temporary array.
 */
public final class ByteSequenceCrc32 implements Checksum {

    private static final int[] CRC_TABLE = new int[256];

    static {
        for (int n = 0; n < 256; n++) {
            int c = n;
            for (int k = 8; --k >= 0;) {
                if ((c & 1) != 0) {
                    c = 0xEDB88320 ^ (c >>> 1);
                } else {
                    c = c >>> 1;
                }
            }
            CRC_TABLE[n] = c;
        }
    }

    private int crc = 0xFFFFFFFF;

    @Override
    public void update(final int b) {
        crc = CRC_TABLE[(crc ^ b) & 0xFF] ^ (crc >>> 8);
    }

    @Override
    public void update(final byte[] b, final int off, final int len) {
        final byte[] validated = Vldtn.requireNonNull(b, "bytes");
        if (off < 0 || len < 0 || off > validated.length
                || ((long) off + (long) len) > validated.length) {
            final long rangeEnd = (long) off + (long) len;
            throw new IllegalArgumentException(String.format(
                    "Range [%d, %d) exceeds array length %d", off, rangeEnd,
                    validated.length));
        }
        crc = updateArrayRange(crc, validated, off, len);
    }

    /**
     * Updates the CRC with the content of the provided {@link ByteSequence}.
     *
     * @param sequence the byte sequence to consume
     */
    public void update(final ByteSequence sequence) {
        final ByteSequence validated = Vldtn.requireNonNull(sequence,
                "sequence");
        if (validated instanceof ByteSequenceView) {
            final ByteSequenceView view = (ByteSequenceView) validated;
            crc = updateArrayRange(crc, view.rawArray(), 0, view.length());
            return;
        }
        if (validated instanceof ByteSequenceSlice) {
            final ByteSequenceSlice slice = (ByteSequenceSlice) validated;
            crc = updateArrayRange(crc, slice.rawArray(), slice.rawOffset(),
                    slice.length());
            return;
        }
        if (validated instanceof MutableBytes) {
            final MutableBytes mutable = (MutableBytes) validated;
            crc = updateArrayRange(crc, mutable.array(), 0, mutable.length());
            return;
        }
        crc = updateSequenceRange(crc, validated);
    }

    private static int updateArrayRange(int crcValue, final byte[] data,
            final int offset, final int length) {
        final int[] table = CRC_TABLE;
        final int end = offset + length;
        final int unrolledEnd = end - 7;
        int index = offset;

        while (index < unrolledEnd) {
            crcValue = table[(crcValue ^ data[index]) & 0xFF]
                    ^ (crcValue >>> 8);
            crcValue = table[(crcValue ^ data[index + 1]) & 0xFF]
                    ^ (crcValue >>> 8);
            crcValue = table[(crcValue ^ data[index + 2]) & 0xFF]
                    ^ (crcValue >>> 8);
            crcValue = table[(crcValue ^ data[index + 3]) & 0xFF]
                    ^ (crcValue >>> 8);
            crcValue = table[(crcValue ^ data[index + 4]) & 0xFF]
                    ^ (crcValue >>> 8);
            crcValue = table[(crcValue ^ data[index + 5]) & 0xFF]
                    ^ (crcValue >>> 8);
            crcValue = table[(crcValue ^ data[index + 6]) & 0xFF]
                    ^ (crcValue >>> 8);
            crcValue = table[(crcValue ^ data[index + 7]) & 0xFF]
                    ^ (crcValue >>> 8);
            index += 8;
        }

        while (index < end) {
            crcValue = table[(crcValue ^ data[index]) & 0xFF]
                    ^ (crcValue >>> 8);
            index++;
        }
        return crcValue;
    }

    private static int updateSequenceRange(int crcValue,
            final ByteSequence sequence) {
        final int[] table = CRC_TABLE;
        final int end = sequence.length();
        final int unrolledEnd = end - 7;
        int index = 0;

        while (index < unrolledEnd) {
            crcValue = table[(crcValue ^ sequence.getByte(index)) & 0xFF]
                    ^ (crcValue >>> 8);
            crcValue = table[(crcValue ^ sequence.getByte(index + 1)) & 0xFF]
                    ^ (crcValue >>> 8);
            crcValue = table[(crcValue ^ sequence.getByte(index + 2)) & 0xFF]
                    ^ (crcValue >>> 8);
            crcValue = table[(crcValue ^ sequence.getByte(index + 3)) & 0xFF]
                    ^ (crcValue >>> 8);
            crcValue = table[(crcValue ^ sequence.getByte(index + 4)) & 0xFF]
                    ^ (crcValue >>> 8);
            crcValue = table[(crcValue ^ sequence.getByte(index + 5)) & 0xFF]
                    ^ (crcValue >>> 8);
            crcValue = table[(crcValue ^ sequence.getByte(index + 6)) & 0xFF]
                    ^ (crcValue >>> 8);
            crcValue = table[(crcValue ^ sequence.getByte(index + 7)) & 0xFF]
                    ^ (crcValue >>> 8);
            index += 8;
        }

        while (index < end) {
            crcValue = table[(crcValue ^ sequence.getByte(index)) & 0xFF]
                    ^ (crcValue >>> 8);
            index++;
        }
        return crcValue;
    }

    @Override
    public long getValue() {
        return (crc ^ 0xFFFFFFFFL) & 0xFFFFFFFFL;
    }

    @Override
    public void reset() {
        crc = 0xFFFFFFFF;
    }
}
