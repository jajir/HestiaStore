package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class FixedPairChunkFilterProviderTest {

    @Test
    void returnsConfiguredEncodingAndDecodingSuppliers() {
        final FixedPairChunkFilterProvider provider = new FixedPairChunkFilterProvider(
                "magic", ChunkFilterMagicNumberWriting::new,
                ChunkFilterMagicNumberValidation::new);
        final ChunkFilterSpec spec = ChunkFilterSpec.ofProvider("magic");

        assertEquals("magic", provider.getProviderId());
        assertInstanceOf(ChunkFilterMagicNumberWriting.class,
                provider.createEncodingSupplier(spec).get());
        assertInstanceOf(ChunkFilterMagicNumberValidation.class,
                provider.createDecodingSupplier(spec).get());
    }

    @Test
    void rejectsMismatchedProviderId() {
        final FixedPairChunkFilterProvider provider = new FixedPairChunkFilterProvider(
                "magic", ChunkFilterMagicNumberWriting::new,
                ChunkFilterMagicNumberValidation::new);
        final ChunkFilterSpec spec = ChunkFilterSpec.ofProvider("crc32");

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> provider.createEncodingSupplier(spec));

        assertEquals(
                "Chunk filter spec provider 'crc32' does not match 'magic'",
                exception.getMessage());
    }
}
