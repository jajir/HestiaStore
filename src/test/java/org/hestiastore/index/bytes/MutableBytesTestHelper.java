package org.hestiastore.index.bytes;

import org.hestiastore.index.Vldtn;

public final class MutableBytesTestHelper {

    private MutableBytesTestHelper() {
        // no instances
    }

    public static MutableBytes copyOf(final ByteSequence sequence) {
        final ByteSequence validated = Vldtn.requireNonNull(sequence,
                "sequence");
        final MutableBytes copy = MutableBytes.allocate(validated.length());
        copy.setBytes(0, validated);
        return copy;
    }
}
