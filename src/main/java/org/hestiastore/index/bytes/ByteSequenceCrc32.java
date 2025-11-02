package org.hestiastore.index.bytes;

import java.util.zip.Checksum;

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
        int c = crc;
        for (int i = off; i < off + len; i++) {
            c = CRC_TABLE[(c ^ b[i]) & 0xFF] ^ (c >>> 8);
        }
        crc = c;
    }

    /**
     * Updates the CRC with the content of the provided {@link ByteSequence}.
     *
     * @param sequence the byte sequence to consume
     */
    public void update(final ByteSequence sequence) {
        int c = crc;
        final int length = sequence.length();
        for (int i = 0; i < length; i++) {
            c = CRC_TABLE[(c ^ sequence.getByte(i)) & 0xFF] ^ (c >>> 8);
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
