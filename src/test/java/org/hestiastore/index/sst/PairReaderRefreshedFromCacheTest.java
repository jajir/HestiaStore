package org.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.NoSuchElementException;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.cache.UniqueCache;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PairReaderRefreshedFromCacheTest {

    private static final Pair<Integer, String> PAIR1 = Pair.of(2, "bbb");
    private static final Pair<Integer, String> PAIR2 = Pair.of(3, "ccc");
    private static final Pair<Integer, String> PAIR3 = Pair.of(4, "ddd");

    private static final TypeDescriptor<String> STRING_TD = new TypeDescriptorShortString();

    @Mock
    private PairIterator<Integer, String> pairIterator;

    @Mock
    private UniqueCache<Integer, String> cache;

    @Test
    void test_get_from_pairIterator_and_not_in_cache() {
        when(pairIterator.hasNext()).thenReturn(true, false);
        when(pairIterator.next()).thenReturn(PAIR1);
        when(cache.get(2)).thenReturn(null);

        try (final PairIteratorRefreshedFromCache<Integer, String> iterator = new PairIteratorRefreshedFromCache<>(
                pairIterator, cache, STRING_TD)) {

            assertTrue(iterator.hasNext());
            assertEquals(PAIR1, iterator.next());
        }
    }

    @Test
    void test_exception_when_reeading_not_existing_element() {
        when(pairIterator.hasNext()).thenReturn(true, false);
        when(pairIterator.next()).thenReturn(PAIR1);
        when(cache.get(2)).thenReturn(null);

        try (final PairIteratorRefreshedFromCache<Integer, String> iterator = new PairIteratorRefreshedFromCache<>(
                pairIterator, cache, STRING_TD)) {

            assertTrue(iterator.hasNext());
            assertEquals(PAIR1, iterator.next());
            assertFalse(iterator.hasNext());
            final Exception e = assertThrows(NoSuchElementException.class,
                    iterator::next);
            assertEquals("No more elements", e.getMessage());
        }
    }

    @Test
    void test_get_from_pairIterator_and_updated_in_cache() {
        when(pairIterator.hasNext()).thenReturn(true);
        when(pairIterator.next()).thenReturn(PAIR1);
        when(cache.get(2)).thenReturn("eee");

        try (final PairIteratorRefreshedFromCache<Integer, String> iterator = new PairIteratorRefreshedFromCache<>(
                pairIterator, cache, STRING_TD)) {

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(2, "eee"), iterator.next());
        }
    }

    @Test
    void test_get_not_in_pairIterator() {
        when(pairIterator.hasNext()).thenReturn(false);

        try (final PairIteratorRefreshedFromCache<Integer, String> iterator = new PairIteratorRefreshedFromCache<>(
                pairIterator, cache, STRING_TD)) {

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_get_from_pairIterator_and_deleted_in_cache_not_other_pair_in_segment() {
        when(pairIterator.hasNext()).thenReturn(true, false);
        when(pairIterator.next()).thenReturn(PAIR1).thenReturn(null);
        when(cache.get(2)).thenReturn(STRING_TD.getTombstone());

        try (final PairIteratorRefreshedFromCache<Integer, String> iterator = new PairIteratorRefreshedFromCache<>(
                pairIterator, cache, STRING_TD)) {

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_two_pairs_are_deleted_third_is_ok() {
        when(pairIterator.hasNext()).thenReturn(true, true, true, false);
        when(pairIterator.next()).thenReturn(PAIR1).thenReturn(PAIR2)
                .thenReturn(PAIR3);
        when(cache.get(2)).thenReturn(STRING_TD.getTombstone());
        when(cache.get(3)).thenReturn(STRING_TD.getTombstone());
        when(cache.get(4)).thenReturn(null);

        try (final PairIteratorRefreshedFromCache<Integer, String> iterator = new PairIteratorRefreshedFromCache<>(
                pairIterator, cache, STRING_TD)) {

            assertTrue(iterator.hasNext());
            assertEquals(PAIR3, iterator.next());
        }
    }

    @Test
    void test_three_pairs_are_deleted() {
        when(pairIterator.hasNext()).thenReturn(true, true, true, false);
        when(pairIterator.next()).thenReturn(PAIR1).thenReturn(PAIR2)
                .thenReturn(PAIR3).thenReturn(null);
        when(cache.get(2)).thenReturn(STRING_TD.getTombstone());
        when(cache.get(3)).thenReturn(STRING_TD.getTombstone());
        when(cache.get(4)).thenReturn(STRING_TD.getTombstone());

        try (final PairIteratorRefreshedFromCache<Integer, String> iterator = new PairIteratorRefreshedFromCache<>(
                pairIterator, cache, STRING_TD)) {

            assertFalse(iterator.hasNext());
        }
    }

}
