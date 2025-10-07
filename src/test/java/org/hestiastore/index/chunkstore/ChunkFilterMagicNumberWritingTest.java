package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hestiastore.index.Bytes;
import org.junit.jupiter.api.Test;

class ChunkFilterMagicNumberWritingTest {

    private static final Bytes PAYLOAD = Bytes.of(new byte[] { 1, 1, 2, 3 });

    @Test
    void apply_should_set_magic_number_constant() {
        final long originalMagic = ChunkHeader.MAGIC_NUMBER + 42;
        final ChunkData input = ChunkData.of(0L, 0L, originalMagic, 1, PAYLOAD);
        final ChunkFilterMagicNumberWriting filter = new ChunkFilterMagicNumberWriting();

        final ChunkData result = filter.apply(input);

        assertEquals(ChunkHeader.MAGIC_NUMBER, result.getMagicNumber());
        assertEquals(PAYLOAD, result.getPayload());
        assertEquals(input.getFlags(), result.getFlags());
        assertEquals(input.getCrc(), result.getCrc());
        assertEquals(input.getVersion(), result.getVersion());
    }
}
