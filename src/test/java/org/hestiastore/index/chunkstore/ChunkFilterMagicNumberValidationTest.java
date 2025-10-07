package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.Bytes;
import org.junit.jupiter.api.Test;

class ChunkFilterMagicNumberValidationTest {

    private static final Bytes PAYLOAD = Bytes.of(new byte[] { 3, 1, 4, 1 });

    @Test
    void apply_should_return_input_when_magic_number_matches() {
        final ChunkData input = ChunkData.of(0L, 0L, ChunkHeader.MAGIC_NUMBER,
                1, PAYLOAD);
        final ChunkFilterMagicNumberValidation filter = new ChunkFilterMagicNumberValidation();

        final ChunkData result = filter.apply(input);

        assertEquals(input, result);
    }

    @Test
    void apply_should_throw_when_magic_number_is_invalid() {
        final ChunkData input = ChunkData.of(0L, 0L,
                ChunkHeader.MAGIC_NUMBER + 17, 1, PAYLOAD);
        final ChunkFilterMagicNumberValidation filter = new ChunkFilterMagicNumberValidation();

        final IllegalStateException exception = assertThrows(
                IllegalStateException.class, () -> filter.apply(input));

        assertEquals(
                String.format(
                        "Invalid chunk magic number. Expected '%s' but was '%s'",
                        ChunkHeader.MAGIC_NUMBER,
                        ChunkHeader.MAGIC_NUMBER + 17),
                exception.getMessage());
    }
}
