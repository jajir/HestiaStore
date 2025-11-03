package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hestiastore.index.bytes.ByteSequenceCrc32;
import org.hestiastore.index.bytes.ByteSequenceView;
import org.junit.jupiter.api.Test;

class ChunkFilterCrc32WritingTest {

    private static final ByteSequenceView PAYLOAD = ByteSequenceView
            .of(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });

    @Test
    void apply_should_update_crc_with_payload_checksum() {
        final ChunkData input = ChunkData.of(0L, 0L, ChunkHeader.MAGIC_NUMBER,
                1, PAYLOAD);
        final ChunkFilterCrc32Writing filter = new ChunkFilterCrc32Writing();

        final ChunkData result = filter.apply(input);

        final ByteSequenceCrc32 crc = new ByteSequenceCrc32();
        crc.update(PAYLOAD);
        assertEquals(crc.getValue(), result.getCrc());
        assertEquals(PAYLOAD, result.getPayload());
        assertEquals(input.getFlags(), result.getFlags());
        assertEquals(input.getMagicNumber(), result.getMagicNumber());
        assertEquals(input.getVersion(), result.getVersion());
    }
}
