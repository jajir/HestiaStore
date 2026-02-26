package org.hestiastore.index.bytes;

/**
 * Singleton {@link ByteSequence} implementation representing an empty sequence.
 */
final class ByteSequenceEmpty implements ByteSequence {

    static final ByteSequenceEmpty INSTANCE = new ByteSequenceEmpty();
    private static final byte[] EMPTY_BYTES = new byte[0];

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
    public ByteSequence slice(final int fromInclusive, final int toExclusive) {
        if (fromInclusive != 0 || toExclusive != 0) {
            throw new IllegalArgumentException(
                    "Slice range must be [0, 0) for an empty sequence.");
        }
        return this;
    }

    @Override
    public byte[] toByteArray() {
        return EMPTY_BYTES;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ByteSequence)) {
            return false;
        }
        return ((ByteSequence) obj).isEmpty();
    }

    @Override
    public int hashCode() {
        return 1;
    }

    @Override
    public String toString() {
        return "ByteSequenceEmpty{}";
    }

}
