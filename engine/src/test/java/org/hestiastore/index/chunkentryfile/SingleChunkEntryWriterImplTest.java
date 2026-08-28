package org.hestiastore.index.chunkentryfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.datatype.NullValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SingleChunkEntryWriterImplTest {

    private SingleChunkEntryWriterImpl<Integer, Long> writer;

    @BeforeEach
    void setUp() {
        writer = new SingleChunkEntryWriterImpl<>(new TypeDescriptorInteger(),
                new TypeDescriptorLong());
    }

    @Test
    void directPutWritesKeyAndValueWithoutEntryWrapper() {
        writer.put(1, 10L);
        writer.put(2, 20L);

        final SingleChunkEntryIterator<Integer, Long> iterator =
                new SingleChunkEntryIterator<>(writer.closeSequence(),
                        new TypeDescriptorInteger(),
                        new TypeDescriptorLong());

        assertTrue(iterator.hasNext());
        assertEquals(Entry.of(1, 10L), iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(Entry.of(2, 20L), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    void directPutRejectsWriteAfterClose() {
        writer.put(1, 10L);
        writer.closeSequence();

        assertThrows(IllegalStateException.class, () -> writer.put(2, 20L));
    }

    @Test
    void primitiveLongPairMatchesGenericWireFormatAcrossSignedRange() {
        final TypeDescriptorLong exact = new TypeDescriptorLong();
        final TypeDescriptorLong genericOnly = new TypeDescriptorLong() {
            // Subclass deliberately selects the generic codec path.
        };
        final SingleChunkEntryWriterImpl<Long, Long> primitive =
                new SingleChunkEntryWriterImpl<>(exact, exact);
        final SingleChunkEntryWriterImpl<Long, Long> generic =
                new SingleChunkEntryWriterImpl<>(genericOnly, genericOnly);
        final long[] keys = { Long.MIN_VALUE, -1L, 0L, 1L, 256L,
                Long.MAX_VALUE };
        for (final long key : keys) {
            primitive.putLongs(key, key ^ 0x5A5A5A5A5A5A5A5AL);
            generic.put(key, key ^ 0x5A5A5A5A5A5A5A5AL);
        }

        assertArrayEquals(generic.closeSequence().toByteArray(),
                primitive.closeSequence().toByteArray());
    }

    @Test
    void primitiveLongKeySupportsGenericValues() {
        final SingleChunkEntryWriterImpl<Long, NullValue> primitive =
                new SingleChunkEntryWriterImpl<>(new TypeDescriptorLong(),
                        new TypeDescriptorNull());

        primitive.putLongKey(-1L, NullValue.NULL);
        primitive.putLongKey(1L, NullValue.NULL);
        final SingleChunkEntryIterator<Long, NullValue> iterator =
                new SingleChunkEntryIterator<>(primitive.closeSequence(),
                        new TypeDescriptorLong(), new TypeDescriptorNull());

        assertEquals(Entry.of(-1L, NullValue.NULL), iterator.next());
        assertEquals(Entry.of(1L, NullValue.NULL), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    void primitiveMethodsRejectIncompatibleDescriptorsAndOrdering() {
        assertThrows(IllegalStateException.class,
                () -> writer.putLongKey(1L, 1L));
        final SingleChunkEntryWriterImpl<Long, NullValue> genericValue =
                new SingleChunkEntryWriterImpl<>(new TypeDescriptorLong(),
                        new TypeDescriptorNull());
        assertThrows(IllegalStateException.class,
                () -> genericValue.putLongs(1L, 1L));
        final SingleChunkEntryWriterImpl<Long, Long> primitive =
                new SingleChunkEntryWriterImpl<>(new TypeDescriptorLong(),
                        new TypeDescriptorLong());
        primitive.putLongs(2L, 1L);
        assertThrows(IllegalArgumentException.class,
                () -> primitive.putLongs(2L, 2L));
        final SingleChunkEntryWriterImpl<Long, Long> bounded =
                new SingleChunkEntryWriterImpl<>(new TypeDescriptorLong(),
                        new TypeDescriptorLong(), 17);
        assertThrows(IllegalStateException.class,
                () -> bounded.putLongs(1L, 1L));
    }
}
