package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hestiastore.index.bytes.ByteSequenceView;
import org.junit.jupiter.api.Test;

class ChunkFilterMagicNumberWritingTest {

    private static final ByteSequenceView PAYLOAD = ByteSequenceView
            .of(new byte[] { 1, 1, 2, 3 });

    @Test
    void apply_should_set_magic_number_constant() {
        final long originalMagic = ChunkHeader.MAGIC_NUMBER + 42;
        final ChunkData input = ChunkData.of(0, 0L, originalMagic, 1, PAYLOAD);
        final ChunkFilterMagicNumberWriting filter = new ChunkFilterMagicNumberWriting();

        final ChunkData result = filter.apply(input);

        assertEquals(ChunkHeader.MAGIC_NUMBER, result.getMagicNumber());
        assertEquals(PAYLOAD, result.getPayload());
        assertEquals(ChunkFilterMagicNumberWriting.FLAG_MASK,
                result.getFlags());
        assertEquals(input.getCrc(), result.getCrc());
        assertEquals(input.getVersion(), result.getVersion());
        assertEquals(ChunkFilterMagicNumberWriting.FLAG_MASK,
                result.getFlags());
    }

    @Test
    void apply_should_set_flag_when_missing() {
        final long otherFlags = 1L << ChunkFilter.BIT_POSITION_SNAPPY_COMPRESSION; // unrelated
        // flag
        final long initialFlags = otherFlags; // magic-number flag not set
        final long originalMagic = ChunkHeader.MAGIC_NUMBER + 7; // will be
                                                                 // overwritten
        final ChunkData input = ChunkData.of(initialFlags, 0L, originalMagic, 1,
                PAYLOAD);
        final ChunkFilterMagicNumberWriting filter = new ChunkFilterMagicNumberWriting();

        final ChunkData result = filter.apply(input);

        final long expectedFlags = initialFlags
                | ChunkFilterMagicNumberWriting.FLAG_MASK;
        assertEquals(expectedFlags, result.getFlags());
        assertEquals(ChunkHeader.MAGIC_NUMBER, result.getMagicNumber());
        assertEquals(PAYLOAD, result.getPayload());
    }
}
