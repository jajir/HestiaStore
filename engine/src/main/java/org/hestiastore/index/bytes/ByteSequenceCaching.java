package org.hestiastore.index.bytes;

import org.hestiastore.index.Vldtn;

/**
 * Abstract base class for byte sequences that cache their byte array
 * representation.
 */
public abstract class ByteSequenceCaching implements ByteSequence {

    private byte[] cachedArray = null;

    @Override
    public final byte[] toByteArray() {
        if (cachedArray == null) {
            cachedArray = Vldtn.requireNonNull(computeByteArray(), "byteArray");
        }
        return cachedArray;
    }

    /**
     * Computes the byte array representation of this sequence.
     *
     * @return the byte array representation
     */
    protected abstract byte[] computeByteArray();

}
