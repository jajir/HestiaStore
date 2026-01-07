package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.junit.jupiter.api.Test;

class MergeWithCacheIteratorTest {

    @Test
    void mergesCacheValuesAndSkipsTombstones() {
        final EntryIterator<Integer, String> mainIterator = new EntryIteratorList<>(
                List.of(Entry.of(1, "one"), Entry.of(3, "three")));
        final List<Integer> cacheKeys = List.of(2, 3);
        final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();
        final Function<Integer, String> cacheGetter = key -> key == 2
                ? "two"
                : valueDescriptor.getTombstone();

        final MergeWithCacheIterator<Integer, String> iterator = new MergeWithCacheIterator<>(
                mainIterator, new TypeDescriptorInteger(), valueDescriptor,
                cacheKeys, cacheGetter);

        final List<Entry<Integer, String>> results = new ArrayList<>();
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        iterator.close();

        assertEquals(2, results.size());
        assertEquals(1, results.get(0).getKey());
        assertEquals("one", results.get(0).getValue());
        assertEquals(2, results.get(1).getKey());
        assertEquals("two", results.get(1).getValue());
    }

    @Test
    void hasNextReturnsFalseAfterConsumption() {
        final EntryIterator<Integer, String> mainIterator = new EntryIteratorList<>(
                List.of(Entry.of(1, "one")));
        final MergeWithCacheIterator<Integer, String> iterator = new MergeWithCacheIterator<>(
                mainIterator, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), List.of(), key -> null);

        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());
        iterator.close();
    }
}
