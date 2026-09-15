package org.hestiastore.index.senku.internal;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.datablockfile.DataBlockSize;

final class LargeFileTestSupport {

    static final DataBlockSize DATA_BLOCK_SIZE = DataBlockSize
            .ofDataBlockSize(1_024);

    private LargeFileTestSupport() {
        // Test utility.
    }

    static ByteSequence page(final String value) {
        return ByteSequences.wrap(value.getBytes(UTF_8));
    }

    static String text(final ByteSequence value) {
        return new String(value.toByteArray(), UTF_8);
    }
}
