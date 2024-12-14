package com.coroptis.index.bloomfilter;

import java.util.Objects;

import org.apache.commons.codec.digest.MurmurHash3;

/**
 * Implementation was taken from <a href=
 * "https://github.com/apache/spark/blob/93251ed77ea1c5d037c64d2292b8760b03c8e181/common/sketch/src/main/java/org/apache/spark/util/sketch/BloomFilterImpl.java">
 * https://github.com/apache/spark/blob/93251ed77ea1c5d037c64d2292b8760b03c8e181/common/sketch/src/main/java/org/apache/spark/util/sketch/BloomFilterImpl.java
 * </a>
 * 
 * @author honza
 *
 */
public class Hash {

    private final BitArray bits;

    private final int numHashFunctions;

    Hash(final BitArray bits, final int numHashFunctions) {
        this.bits = Objects.requireNonNull(bits);
        if (numHashFunctions <= 0) {
            throw new IllegalArgumentException(String.format(
                    "Number of hash function cant be '%s'", numHashFunctions));
        }
        this.numHashFunctions = numHashFunctions;
    }

    public boolean store(final byte[] data) {
        if (data == null) {
            throw new NullPointerException("No data");
        }
        if (data.length == 0) {
            throw new IllegalArgumentException("Zero size of byte array");
        }
        final long bitSize = bits.bitSize();
        if (bitSize == 0) {
            return true;
        }

        int h1 = MurmurHash3.hash32x86(data, 0, data.length, 0);
        int h2 = MurmurHash3.hash32x86(data, 0, data.length, h1);

        boolean bitsChanged = false;
        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = h1 + (i * h2);
            // Flip all the bits if it's negative (guaranteed positive number)
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            bitsChanged |= bits.setBit((int) (combinedHash % bitSize));
        }
        return bitsChanged;
    }

    /**
     * When function return that record is not stored in index. When function
     * replay that data are stored in index there is chance that record is not
     * in index.
     * 
     * @param data required data
     * @return return <code>true</code> when it's sure that data are not in
     *         index. Otherwise return <code>false</code>.
     */
    public boolean isNotStored(final byte[] data) {
        if (data == null) {
            throw new NullPointerException("No data");
        }
        if (data.length == 0) {
            throw new IllegalArgumentException("Zero size of byte array");
        }
        long bitSize = bits.bitSize();
        if (bitSize == 0) {
            // if there are no bits set, then the data is not stored
            return false;
        }

        int h1 = MurmurHash3.hash32x86(data, 0, data.length, 0);
        int h2 = MurmurHash3.hash32x86(data, 0, data.length, h1);

        boolean bitsChanged = false;
        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = h1 + (i * h2);
            // Flip all the bits if it's negative (guaranteed positive number)
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            bitsChanged |= bits.get((int) (combinedHash % bitSize));
        }
        return bitsChanged;
    }

    public byte[] getData() {
        return bits.getByteArray();
    }

}
