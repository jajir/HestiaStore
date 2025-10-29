package org.hestiastore.index.bloomfilter;

import org.apache.commons.codec.digest.MurmurHash3;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

/**
 * Implementation was taken from <a href=
 * "https://github.com/apache/spark/blob/93251ed77ea1c5d037c64d2292b8760b03c8e181/common/sketch/src/main/java/org/apache/spark/util/sketch/BloomFilterImpl.java">
 * https://github.com/apache/spark/blob/93251ed77ea1c5d037c64d2292b8760b03c8e181/common/sketch/src/main/java/org/apache/spark/util/sketch/BloomFilterImpl.java
 * </a>
 * 
 * @author honza
 *
 */
public final class Hash {

    private final BitArray bitArray;

    private final int numHashFunctions;

    Hash(final BitArray bits, final int numHashFunctions) {
        this.bitArray = Vldtn.requireNonNull(bits, "bits");
        if (numHashFunctions <= 0) {
            throw new IllegalArgumentException(String.format(
                    "Number of hash function cant be '%s'", numHashFunctions));
        }
        this.numHashFunctions = numHashFunctions;
    }

    public boolean store(final Bytes data) {
        Vldtn.requireNonNull(data, "data");
        final byte[] raw = data.toByteArray();
        if (raw.length == 0) {
            throw new IllegalArgumentException("Zero size of byte array");
        }
        final long bitSize = bitArray.bitSize();
        if (bitSize == 0) {
            return true;
        }

        int h1 = MurmurHash3.hash32x86(raw, 0, raw.length, 0);
        int h2 = MurmurHash3.hash32x86(raw, 0, raw.length, h1);

        boolean bitsChanged = false;
        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = h1 + (i * h2);
            // Flip all the bits if it's negative (guaranteed positive number)
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            bitsChanged |= bitArray.setBit((int) (combinedHash % bitSize));
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
    public boolean isNotStored(final Bytes data) {
        return !isProbablyStored(data);
    }

    public boolean isProbablyStored(final Bytes data) {
        Vldtn.requireNonNull(data, "data");
        final byte[] raw = data.toByteArray();
        if (raw.length == 0) {
            throw new IllegalArgumentException("Zero size of byte array");
        }
        long bitSize = bitArray.bitSize();
        if (bitSize == 0) {
            // if there are no bits set, then the data is not stored
            return true;
        }

        int h1 = MurmurHash3.hash32x86(raw, 0, raw.length, 0);
        int h2 = MurmurHash3.hash32x86(raw, 0, raw.length, h1);

        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = h1 + (i * h2);
            // Flip all the bits if it's negative (guaranteed positive number)
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            final boolean bitSet = bitArray.get((int) (combinedHash % bitSize));
            if (!bitSet) {
                /**
                 * There is at least one bit that is not set. So I can say that
                 * data is not stored into index.
                 */
                return false;
            }
        }
        /**
         * All bits are set, so I can say that data is probably stored in index.
         */
        return true;
    }

    public Bytes getData() {
        return bitArray.getBytes();
    }

}
