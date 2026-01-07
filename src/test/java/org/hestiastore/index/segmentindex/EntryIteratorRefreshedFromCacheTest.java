package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.cache.UniqueCache;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EntryIteratorRefreshedFromCacheTest {

    @Mock
    private EntryIterator<Integer, String> entryIterator;

    @Mock
    private UniqueCache<Integer, String> cache;

    private final TypeDescriptorString valueDescriptor = new TypeDescriptorString();

    @Test
    void returnsCachedValueWhenPresent() {
        when(entryIterator.hasNext()).thenReturn(true, false);
        when(entryIterator.next()).thenReturn(Entry.of(1, "old"));
        when(cache.get(1)).thenReturn("new");

        try (EntryIteratorRefreshedFromCache<Integer, String> iterator = new EntryIteratorRefreshedFromCache<>(
                entryIterator, cache, valueDescriptor)) {
            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(1, "new"), iterator.next());
        }
    }

    @Test
    void skipsTombstoneValues() {
        when(entryIterator.hasNext()).thenReturn(true, false);
        when(entryIterator.next()).thenReturn(Entry.of(1, "old"));
        when(cache.get(1)).thenReturn(TypeDescriptorString.TOMBSTONE_VALUE);

        try (EntryIteratorRefreshedFromCache<Integer, String> iterator = new EntryIteratorRefreshedFromCache<>(
                entryIterator, cache, valueDescriptor)) {
            assertFalse(iterator.hasNext());
        }
    }
}
