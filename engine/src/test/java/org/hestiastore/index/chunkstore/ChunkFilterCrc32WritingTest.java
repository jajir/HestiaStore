package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.commons.codec.digest.PureJavaCrc32;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.Test;

class ChunkFilterCrc32WritingTest {

    private static final ByteSequence PAYLOAD = ByteSequences
            .wrap(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });

    @Test
    void apply_should_update_crc_with_payload_checksum() {
        final ChunkData input = ChunkData.ofSequence(0L, 0L,
                ChunkHeader.MAGIC_NUMBER,
                1, PAYLOAD);
        final ChunkFilterCrc32Writing filter = new ChunkFilterCrc32Writing();

        final ChunkData result = filter.apply(input);

        final PureJavaCrc32 crc = new PureJavaCrc32();
        crc.update(PAYLOAD.toByteArrayCopy());
        assertEquals(crc.getValue(), result.getCrc());
        assertArrayEquals(PAYLOAD.toByteArrayCopy(),
                result.getPayloadSequence().toByteArrayCopy());
        assertEquals(input.getFlags(), result.getFlags());
        assertEquals(input.getMagicNumber(), result.getMagicNumber());
        assertEquals(input.getVersion(), result.getVersion());
    }
}
