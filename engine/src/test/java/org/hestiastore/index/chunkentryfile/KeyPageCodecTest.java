package org.hestiastore.index.chunkentryfile;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.MemFileReader;
import org.junit.jupiter.api.Test;

class KeyPageCodecTest {
    @Test
    void fixedWeightParametersRemainImmutableAndRequireBuiltInLongs() {
        final long[] masks = { 3 };
        final var codec = KeyPageCodecs.longFixedWeightDeltaVarint(4, 2, masks,
                0);
        masks[0] = 0;
        codec.getFixedWeightParityMasks()[0] = 0;
        assertEquals(4, codec.getFixedWeightBitCount());
        assertEquals(2, codec.getFixedWeightSetBitCount());
        assertArrayEquals(new long[] { 3 }, codec.getFixedWeightParityMasks());
        assertEquals(0, codec.getFixedWeightParitySyndrome());
        assertThrows(IndexException.class,
                () -> codec.validate(new TypeDescriptorInteger()));
        assertThrows(IndexException.class,
                () -> KeyPageCodecs.prefix().getFixedWeightBitCount());
        assertDoesNotThrow(() -> codec.validate(new TypeDescriptorLong()));
        final var decoder = codec.createReader(new TypeDescriptorLong());
        try (var input = new MemFileReader(new byte[8])) {
            assertEquals(3L, decoder.read(input));
            assertNull(decoder.read(input));
        }
    }

    @Test
    void deltaRejectsDifferentTypesAndCustomLongSemantics() {
        final var codec = KeyPageCodecs.longDeltaVarint();
        final var custom = new TypeDescriptorLong() {
        };
        assertThrows(IndexException.class,
                () -> codec.validate(new TypeDescriptorInteger()));
        assertThrows(IndexException.class, () -> codec.validate(custom));
        assertThrows(IllegalArgumentException.class,
                () -> codec.validate(null));
        assertDoesNotThrow(() -> codec.validate(new TypeDescriptorLong()));
    }

    @Test
    void readersHaveIndependentPageState() {
        final var codec = KeyPageCodecs.longDeltaVarint();
        final var keys = new TypeDescriptorLong();
        final var first = codec.createReader(keys);
        final var second = codec.createReader(keys);
        try (var input = new MemFileReader(
                new byte[] { 0, 0, 0, 0, 0, 0, 0, 10, 1 });
                var other = new MemFileReader(
                        new byte[] { 0, 0, 0, 0, 0, 0, 0, 20, 2 })) {
            assertEquals(10L, first.read(input));
            assertEquals(20L, second.read(other));
            assertEquals(11L, first.read(input));
            assertEquals(22L, second.read(other));
            assertNull(first.read(input));
        }
    }

    @Test
    void prefixRetainsGenericDescriptorEncoding() {
        final var keys = new TypeDescriptorInteger();
        final var writer = new SingleChunkEntryWriterImpl<>(keys,
                new TypeDescriptorNull());
        writer.put(123, NullValue.NULL);
        try (var reader = new MemFileReader(writer.closeSequence())) {
            assertEquals(123, KeyPageCodecs.<Integer>prefix().createReader(keys)
                    .read(reader));
        }
    }
}
