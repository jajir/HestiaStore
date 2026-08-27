package org.hestiastore.index.chunkentryfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
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
}
