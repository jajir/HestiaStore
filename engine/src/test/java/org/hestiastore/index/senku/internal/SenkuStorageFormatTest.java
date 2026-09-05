package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.hestiastore.index.chunkentryfile.KeyPageCodecs;
import org.hestiastore.index.chunkstore.Compression;
import org.junit.jupiter.api.Test;

class SenkuStorageFormatTest {
    @Test
    void retainsFullImmutableRankDomainWithoutRebuildingItsTables() {
        final var codec = KeyPageCodecs.longFixedWeightDeltaVarint(7, 3,
                new long[] { 3, 12 }, 1);
        final var format = new SenkuStorageFormat(codec, Compression.zstd(3));
        assertSame(codec, format.keyCodec());
        assertEquals(7, format.keyCodec().getFixedWeightBitCount());
        assertEquals(3, format.keyCodec().getFixedWeightSetBitCount());
        assertArrayEquals(new long[] { 3, 12 },
                format.keyCodec().getFixedWeightParityMasks());
        assertEquals(1, format.keyCodec().getFixedWeightParitySyndrome());
    }

    @Test
    void keepsIndependentKeyAndCompressionChoices() {
        final var defaults = SenkuStorageFormat.createDefault();
        assertFalse(defaults.keyCodec().isLongDeltaVarint());
        assertEquals(3, defaults.compression().getLevel());
        final var custom = new SenkuStorageFormat(
                KeyPageCodecs.longDeltaVarint(), Compression.none());
        assertTrue(custom.keyCodec().isLongDeltaVarint());
        assertEquals(0, custom.compression().getLevel());
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuStorageFormat(null, Compression.none()));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuStorageFormat(KeyPageCodecs.prefix(), null));
    }
}
