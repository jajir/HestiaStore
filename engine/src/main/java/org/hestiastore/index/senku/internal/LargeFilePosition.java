package org.hestiastore.index.senku.internal;

import org.hestiastore.index.Vldtn;

/**
 * Opaque packed position within one Senku large file.
 */
final class LargeFilePosition {

    private static final int PART_SHIFT = 31;
    private static final long MAX_PART_NUMBER = 0xffff_ffffL;
    private static final long LOCAL_POSITION_MASK = 0x7fff_ffffL;

    private final long packed;

    private LargeFilePosition(final long packed) {
        this.packed = packed;
    }

    /**
     * Packs a physical part number and local signed-int cell position.
     *
     * @param partNumber   non-negative 32-bit physical part number
     * @param localPosition non-negative local cell position
     * @return opaque packed position
     */
    static LargeFilePosition of(final long partNumber,
            final int localPosition) {
        final long validatedPart = Vldtn.requireGreaterThanOrEqualToZero(
                partNumber, "partNumber");
        Vldtn.requireTrue(validatedPart <= MAX_PART_NUMBER,
                "Property 'partNumber' must fit in 32 unsigned bits");
        final int validatedLocal = Vldtn.requireGreaterThanOrEqualToZero(
                localPosition, "localPosition");
        final long packed = validatedPart << PART_SHIFT
                | validatedLocal & LOCAL_POSITION_MASK;
        return new LargeFilePosition(packed);
    }

    /**
     * Restores a position persisted as one non-negative long.
     *
     * @param packed non-negative packed position
     * @return decoded opaque position
     */
    static LargeFilePosition fromPacked(final long packed) {
        return new LargeFilePosition(Vldtn.requireGreaterThanOrEqualToZero(
                packed, "packed"));
    }

    long getPacked() {
        return packed;
    }

    long getPartNumber() {
        return packed >>> PART_SHIFT;
    }

    int getLocalPosition() {
        return (int) (packed & LOCAL_POSITION_MASK);
    }
}
