package org.hestiastore.index.bytes;

import org.hestiastore.index.Vldtn;

/**
 * Base implementation that validates bulk-copy arguments before delegating to
 * a representation-specific copy operation.
 */
public abstract class AbstractByteSequence implements ByteSequence {

    /**
     * Creates a byte-sequence base instance.
     */
    protected AbstractByteSequence() {
        // extension point
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void copyTo(final int sourceOffset, final byte[] target,
            final int targetOffset, final int length) {
        final byte[] validatedTarget = Vldtn.requireNonNull(target, "target");
        validateRange(sourceOffset, length, length(), "sourceOffset");
        validateRange(targetOffset, length, validatedTarget.length,
                "targetOffset");
        if (length == 0) {
            return;
        }
        copyToWithValidatedInputs(sourceOffset, validatedTarget, targetOffset,
                length);
    }

    /**
     * Copies a range after {@link #copyTo(int, byte[], int, int)} has validated
     * all arguments. The default implementation copies through
     * {@link #getByte(int)}.
     *
     * @param sourceOffset valid zero-based offset in this sequence
     * @param target       validated destination array
     * @param targetOffset valid zero-based offset in {@code target}
     * @param length       positive number of bytes to copy
     */
    protected void copyToWithValidatedInputs(final int sourceOffset,
            final byte[] target, final int targetOffset, final int length) {
        for (int index = 0; index < length; index++) {
            target[targetOffset + index] = getByte(sourceOffset + index);
        }
    }

    private static void validateRange(final int offset, final int length,
            final int capacity, final String propertyName) {
        if (offset < 0) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' must not be negative.", propertyName));
        }
        if (length < 0) {
            throw new IllegalArgumentException(
                    "Property 'length' must not be negative.");
        }
        final long end = (long) offset + length;
        if (offset > capacity || end > capacity) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' with length %d exceeds capacity %d",
                    propertyName, length, capacity));
        }
    }
}
