package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.commons.codec.digest.PureJavaCrc32;
import org.hestiastore.index.Bytes;
import org.junit.jupiter.api.Test;

class ChunkFilterCrc32ValidationTest {

    private static final Bytes PAYLOAD = Bytes
            .of(new byte[] { 9, 8, 7, 6, 5, 4, 3, 2 });

    @Test
    void apply_should_return_input_when_crc_matches() {
        final long crcValue = calculateCrc(PAYLOAD);
        final ChunkData input = ChunkData.of(0L, crcValue,
                ChunkHeader.MAGIC_NUMBER, 1, PAYLOAD);
        final ChunkFilterCrc32Validation filter = new ChunkFilterCrc32Validation();

        final ChunkData result = filter.apply(input);

        assertEquals(input, result);
    }

    @Test
    void apply_should_throw_when_crc_does_not_match() {
        final long crcValue = calculateCrc(PAYLOAD) + 1;
        final ChunkData input = ChunkData.of(0L, crcValue,
                ChunkHeader.MAGIC_NUMBER, 1, PAYLOAD);
        final ChunkFilterCrc32Validation filter = new ChunkFilterCrc32Validation();

        final IllegalStateException exception = assertThrows(
                IllegalStateException.class, () -> filter.apply(input));

        assertEquals(
                String.format("Invalid CRC32. Expected '%s' but calculated '%s'",
                        crcValue, calculateCrc(PAYLOAD)),
                exception.getMessage());
    }

    private static long calculateCrc(final Bytes data) {
        final PureJavaCrc32 crc = new PureJavaCrc32();
        crc.update(data.getData());
        return crc.getValue();
    }
}
