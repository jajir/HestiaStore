package org.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.NoSuchElementException;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.cache.UniqueCache;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EntryReaderRefreshedFromCacheTest {

    private static final Entry<Integer, String> ENTRY1 = Entry.of(2, "bbb");
    private static final Entry<Integer, String> ENTRY2 = Entry.of(3, "ccc");
    private static final Entry<Integer, String> ENTRY3 = Entry.of(4, "ddd");

    private static final TypeDescriptor<String> STRING_TD = new TypeDescriptorShortString();

    @Mock
    private EntryIterator<Integer, String> entryIterator;

    @Mock
    private UniqueCache<Integer, String> cache;

    @Test
    void test_get_from_entryIterator_and_not_in_cache() {
        when(entryIterator.hasNext()).thenReturn(true, false);
        when(entryIterator.next()).thenReturn(ENTRY1);
        when(cache.get(2)).thenReturn(null);

        try (final EntryIteratorRefreshedFromCache<Integer, String> iterator = new EntryIteratorRefreshedFromCache<>(
                entryIterator, cache, STRING_TD)) {

            assertTrue(iterator.hasNext());
            assertEquals(ENTRY1, iterator.next());
        }
    }

    @Test
    void test_exception_when_reeading_not_existing_element() {
        when(entryIterator.hasNext()).thenReturn(true, false);
        when(entryIterator.next()).thenReturn(ENTRY1);
        when(cache.get(2)).thenReturn(null);

        try (final EntryIteratorRefreshedFromCache<Integer, String> iterator = new EntryIteratorRefreshedFromCache<>(
                entryIterator, cache, STRING_TD)) {

            assertTrue(iterator.hasNext());
            assertEquals(ENTRY1, iterator.next());
            assertFalse(iterator.hasNext());
            final Exception e = assertThrows(NoSuchElementException.class,
                    iterator::next);
            assertEquals("No more elements", e.getMessage());
        }
    }

    @Test
    void test_get_from_entryIterator_and_updated_in_cache() {
        when(entryIterator.hasNext()).thenReturn(true);
        when(entryIterator.next()).thenReturn(ENTRY1);
        when(cache.get(2)).thenReturn("eee");

        try (final EntryIteratorRefreshedFromCache<Integer, String> iterator = new EntryIteratorRefreshedFromCache<>(
                entryIterator, cache, STRING_TD)) {

            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(2, "eee"), iterator.next());
        }
    }

    @Test
    void test_get_not_in_entryIterator() {
        when(entryIterator.hasNext()).thenReturn(false);

        try (final EntryIteratorRefreshedFromCache<Integer, String> iterator = new EntryIteratorRefreshedFromCache<>(
                entryIterator, cache, STRING_TD)) {

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_get_from_entryIterator_and_deleted_in_cache_not_other_entry_in_segment() {
        when(entryIterator.hasNext()).thenReturn(true, false);
        when(entryIterator.next()).thenReturn(ENTRY1).thenReturn(null);
        when(cache.get(2)).thenReturn(STRING_TD.getTombstone());

        try (final EntryIteratorRefreshedFromCache<Integer, String> iterator = new EntryIteratorRefreshedFromCache<>(
                entryIterator, cache, STRING_TD)) {

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_two_entries_are_deleted_third_is_ok() {
        when(entryIterator.hasNext()).thenReturn(true, true, true, false);
        when(entryIterator.next()).thenReturn(ENTRY1).thenReturn(ENTRY2)
                .thenReturn(ENTRY3);
        when(cache.get(2)).thenReturn(STRING_TD.getTombstone());
        when(cache.get(3)).thenReturn(STRING_TD.getTombstone());
        when(cache.get(4)).thenReturn(null);

        try (final EntryIteratorRefreshedFromCache<Integer, String> iterator = new EntryIteratorRefreshedFromCache<>(
                entryIterator, cache, STRING_TD)) {

            assertTrue(iterator.hasNext());
            assertEquals(ENTRY3, iterator.next());
        }
    }

    @Test
    void test_three_entries_are_deleted() {
        when(entryIterator.hasNext()).thenReturn(true, true, true, false);
        when(entryIterator.next()).thenReturn(ENTRY1).thenReturn(ENTRY2)
                .thenReturn(ENTRY3).thenReturn(null);
        when(cache.get(2)).thenReturn(STRING_TD.getTombstone());
        when(cache.get(3)).thenReturn(STRING_TD.getTombstone());
        when(cache.get(4)).thenReturn(STRING_TD.getTombstone());

        try (final EntryIteratorRefreshedFromCache<Integer, String> iterator = new EntryIteratorRefreshedFromCache<>(
                entryIterator, cache, STRING_TD)) {

            assertFalse(iterator.hasNext());
        }
    }

}
