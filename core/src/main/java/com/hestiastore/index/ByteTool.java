package com.hestiastore.index;

/**
 * Simple operations with byte arrays.
 * 
 * @author jan
 * 
 */
public class ByteTool {

    /**
     * Count how many byte is same in two byte arrays.
     * 
     * @param array1 required first array
     * @param array2 required second array
     * @return number of same bytes
     */
    public int howMuchBytesIsSame(final byte[] array1, final byte[] array2) {
        if (array1 == null) {
            throw new NullPointerException("First byte array is null.");
        }
        if (array2 == null) {
            throw new NullPointerException("Second byte array is null.");
        }
        int sameBytes = 0;
        while (true) {
            if (sameBytes >= array1.length) {
                return sameBytes;
            }
            if (sameBytes >= array2.length) {
                return sameBytes;
            }
            if (array1[sameBytes] == array2[sameBytes]) {
                sameBytes++;
            } else {
                return sameBytes;
            }
        }
    }

    /**
     * Get part of byte array after given index.
     * 
     * @param index required index in byte array
     * @param full  required byte array
     * @return byte array
     */
    public byte[] getRemainingBytesAfterIndex(final int index,
            final byte[] full) {
        final byte[] out = new byte[full.length - index];
        System.arraycopy(full, index, out, 0, out.length);
        return out;
    }

}
