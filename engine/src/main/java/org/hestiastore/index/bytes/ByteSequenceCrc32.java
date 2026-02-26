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
        int c = crc;
        for (int i = off; i < off + len; i++) {
            c = CRC_TABLE[(c ^ validated[i]) & 0xFF] ^ (c >>> 8);
        }
        crc = c;
    }

    /**
     * Updates the CRC with the content of the provided {@link ByteSequence}.
     *
     * @param sequence the byte sequence to consume
     */
    public void update(final ByteSequence sequence) {
        final ByteSequence validated = Vldtn.requireNonNull(sequence,
                "sequence");
        int c = crc;
        final int length = validated.length();
        for (int i = 0; i < length; i++) {
            c = CRC_TABLE[(c ^ validated.getByte(i)) & 0xFF] ^ (c >>> 8);
        }
        crc = c;
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
