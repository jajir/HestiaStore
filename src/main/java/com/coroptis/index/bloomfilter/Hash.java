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

    Hash(final BitArray bits, final int numHashFunctions){
this.bits = Objects.requireNonNull(bits);
if(numHashFunctions<=0){
throw new IllegalArgumentException(String.format("Number of hash function cant be '%s'",numHashFunctions));    
}
this.numHashFunctions=numHashFunctions;
    }

    public boolean store(final byte[] item) {
        int h1 = MurmurHash3.hash32x86(item, 0, item.length, 0);
        int h2 = MurmurHash3.hash32x86(item, 0, item.length, h1);

        long bitSize = bits.bitSize();
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

    public boolean isNotStored(final byte[] item) {
        int h1 = MurmurHash3.hash32x86(item, 0, item.length, 0);
        int h2 = MurmurHash3.hash32x86(item, 0, item.length, h1);

        long bitSize = bits.bitSize();
        boolean bitsChanged = false;
        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = h1 + (i * h2);
            // Flip all the bits if it's negative (guaranteed positive number)
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            System.out.println(combinedHash + " "+ bitSize + "  " + ((int) (combinedHash % bitSize)));
            bitsChanged |= bits.get((int) (combinedHash % bitSize));
        }
        return bitsChanged;
    }

}
