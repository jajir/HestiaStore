package org.hestiastore.index.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Comparator;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.sorteddatafile.SortedDataFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class UniqueCacheBuilderTest {

    @Mock
    private SortedDataFile<Integer, String> sdf;

    @Test
    void test_builder_static_factory() {
        final UniqueCacheBuilder<Integer, String> builder = UniqueCache.builder();
        assertNotNull(builder);
    }

    @Test
    void test_build_populates_cache_and_deduplicates_latest_wins() {
        final List<Entry<Integer, String>> data = List.of( // sorted input with duplicates
                Entry.of(1, "A"),
                Entry.of(2, "B0"),
                Entry.of(2, "B1"),
                Entry.of(3, "C0"),
                Entry.of(3, "C1"),
                Entry.of(4, "D"));

        final EntryIteratorWithCurrent<Integer, String> iterator = new EntryIteratorList<>(data);
        when(sdf.openIterator()).thenReturn(iterator);

        final Comparator<Integer> cmp = Integer::compareTo;

        final UniqueCache<Integer, String> cache = UniqueCache.<Integer, String>builder()
                .withKeyComparator(cmp)
                .withDataFile(sdf)
                .build();

        // Size is by unique key count
        assertEquals(4, cache.size());

        // Sorted keys and last value per key are preserved
        final List<Entry<Integer, String>> out = cache.getAsSortedList();
        assertEquals(Entry.of(1, "A"), out.get(0));
        assertEquals(Entry.of(2, "B1"), out.get(1));
        assertEquals(Entry.of(3, "C1"), out.get(2));
        assertEquals(Entry.of(4, "D"), out.get(3));

        // Iterator provided to builder is closed
        // EntryIteratorList exposes wasClosed() via AbstractCloseableResource
        //noinspection resource
        assertEquals(true, ((EntryIteratorList<Integer, String>) iterator).wasClosed());
    }

    @Test
    void test_build_null_keyComparator_throws() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> UniqueCache.<Integer, String>builder()
                        .withKeyComparator(null)
                        .withDataFile(sdf)
                        .build());

        assertEquals("Property 'keyComparator' must not be null.", e.getMessage());
    }

    @Test
    void test_build_missing_dataFile_throws() {
        final Comparator<Integer> cmp = Integer::compareTo;
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> UniqueCache.<Integer, String>builder()
                        .withKeyComparator(cmp)
                        // intentionally not setting data file
                        .build());
        assertEquals("Property 'sdf' must not be null.", e.getMessage());
    }

    @Test
    void test_buildEmpty_with_empty_dataFile_returns_empty_cache() {
        final UniqueCache<Integer, String> cache = UniqueCache
                .<Integer, String>builder()
                .withKeyComparator(Integer::compareTo)
                .buildEmpty();

        assertEquals(0, cache.size());
    }

    @Test
    void test_build_with_initial_capacity() {
        final List<Entry<Integer, String>> data = List.of(
                Entry.of(10, "A"), Entry.of(20, "B"), Entry.of(30, "C"));
        final EntryIteratorWithCurrent<Integer, String> iterator = new EntryIteratorList<>(data);
        when(sdf.openIterator()).thenReturn(iterator);

        final UniqueCache<Integer, String> cache = UniqueCache.<Integer, String>builder()
                .withKeyComparator(Integer::compareTo)
                .withDataFile(sdf)
                .withInitialCapacity(128)
                .build();

        assertEquals(3, cache.size());
        final List<Entry<Integer, String>> out = cache.getAsSortedList();
        assertEquals(Entry.of(10, "A"), out.get(0));
        assertEquals(Entry.of(20, "B"), out.get(1));
        assertEquals(Entry.of(30, "C"), out.get(2));
    }

    @Test
    void test_withInitialCapacity_zero_throws() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> UniqueCache.<Integer, String>builder()
                        .withInitialCapacity(0));
        assertEquals("Property 'initialCapacity' must be greater than 0",
                e.getMessage());
    }

    @Test
    void test_withInitialCapacity_negative_throws() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> UniqueCache.<Integer, String>builder()
                        .withInitialCapacity(-10));
        assertEquals("Property 'initialCapacity' must be greater than 0",
                e.getMessage());
    }

    @Test
    void test_build_with_threadSafe_true_returns_synchronized_cache() {
        final List<Entry<Integer, String>> data = List.of(
                Entry.of(1, "a"), Entry.of(2, "b"));
        final EntryIteratorWithCurrent<Integer, String> iterator = new EntryIteratorList<>(
                data);
        when(sdf.openIterator()).thenReturn(iterator);

        final UniqueCache<Integer, String> cache = UniqueCache
                .<Integer, String>builder()
                .withKeyComparator(Integer::compareTo)
                .withDataFile(sdf)
                .withThreadSafe(true)
                .build();

        assertTrue(cache instanceof UniqueCacheSynchronizenizedAdapter);
    }

    @Test
    void test_build_with_threadSafe_false_returns_plain_cache() {
        final List<Entry<Integer, String>> data = List.of(Entry.of(1, "a"));
        final EntryIteratorWithCurrent<Integer, String> iterator = new EntryIteratorList<>(
                data);
        when(sdf.openIterator()).thenReturn(iterator);

        final UniqueCache<Integer, String> cache = UniqueCache
                .<Integer, String>builder()
                .withKeyComparator(Integer::compareTo)
                .withDataFile(sdf)
                .withThreadSafe(false)
                .build();

        assertFalse(cache instanceof UniqueCacheSynchronizenizedAdapter);
    }

    @Test
    void test_buildEmpty_without_dataFile_returns_empty_cache() {
        final UniqueCache<Integer, String> cache = UniqueCache
                .<Integer, String>builder()
                .withKeyComparator(Integer::compareTo)
                .withInitialCapacity(8)
                .buildEmpty();

        assertEquals(0, cache.size());
    }

    @Test
    void test_buildEmpty_threadSafe_false_returns_plain_cache() {
        final UniqueCache<Integer, String> cache = UniqueCache
                .<Integer, String>builder()
                .withKeyComparator(Integer::compareTo)
                .withThreadSafe(false)
                .buildEmpty();

        assertFalse(cache instanceof UniqueCacheSynchronizenizedAdapter);
    }

    @Test
    void test_buildEmpty_threadSafe_true_returns_synchronized_cache() {
        final UniqueCache<Integer, String> cache = UniqueCache
                .<Integer, String>builder()
                .withKeyComparator(Integer::compareTo)
                .withThreadSafe(true)
                .buildEmpty();

        assertTrue(cache instanceof UniqueCacheSynchronizenizedAdapter);
    }
}
