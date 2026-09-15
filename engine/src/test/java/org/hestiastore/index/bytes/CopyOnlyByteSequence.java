package org.hestiastore.index.bytes;

/**
 * Test sequence that fails if a bulk-copy operation materializes a child array
 * or falls back to copying one byte at a time.
 */
final class CopyOnlyByteSequence extends AbstractByteSequence {

    private final byte[] bytes;

    CopyOnlyByteSequence(final byte[] bytes) {
        this.bytes = bytes;
    }

    /** {@inheritDoc} */
    @Override
    public int length() {
        return bytes.length;
    }

    /**
     * Fails when the code under test attempts to copy individual bytes.
     */
    @Override
    public byte getByte(final int index) {
        throw new AssertionError("Expected a bulk copy of child bytes");
    }

    /** {@inheritDoc} */
    @Override
    public ByteSequence slice(final int fromInclusive, final int toExclusive) {
        return ByteSequences.viewOf(bytes, fromInclusive, toExclusive);
    }

    /**
     * Fails when the code under test attempts to allocate a child array.
     */
    @Override
    public byte[] toByteArray() {
        throw new AssertionError("Child arrays must not be materialized");
    }

    /** {@inheritDoc} */
    @Override
    protected void copyToWithValidatedInputs(final int sourceOffset,
            final byte[] target, final int targetOffset, final int length) {
        System.arraycopy(bytes, sourceOffset, target, targetOffset, length);
    }
}
