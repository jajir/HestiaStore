package org.hestiastore.index.bytes;

/**
 * Singleton {@link ByteSequence} implementation representing an empty sequence.
 */
final class ByteSequenceEmpty implements ByteSequence {

    static final ByteSequenceEmpty INSTANCE = new ByteSequenceEmpty();

    private ByteSequenceEmpty() {
        // singleton
    }

    @Override
    public int length() {
        return 0;
    }

    @Override
    public byte getByte(final int index) {
        throw new IllegalArgumentException(
                "Property 'index' must be between 0 and -1 (inclusive). Got: "
                        + index);
    }

    @Override
    public void copyTo(final int sourceOffset, final byte[] target,
            final int targetOffset, final int length) {
        if (target == null) {
            throw new IllegalArgumentException(
                    "Property 'target' must not be null.");
        }
        if (sourceOffset != 0) {
            throw new IllegalArgumentException(
                    "Property 'sourceOffset' must be 0 for an empty sequence.");
        }
        if (length != 0) {
            throw new IllegalArgumentException(
                    "Property 'length' must be 0 for an empty sequence.");
        }
        if (targetOffset < 0 || targetOffset > target.length) {
            throw new IllegalArgumentException(String.format(
                    "Property 'targetOffset' must be between 0 and %d (inclusive). Got: %d",
                    target.length, targetOffset));
        }
    }

    @Override
    public ByteSequence slice(final int fromInclusive, final int toExclusive) {
        if (fromInclusive != 0 || toExclusive != 0) {
            throw new IllegalArgumentException(
                    "Slice range must be [0, 0) for an empty sequence.");
        }
        return this;
    }

    @Override
    public byte[] toByteArray() {
        return new byte[0];
    }

}
