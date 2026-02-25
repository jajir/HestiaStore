package org.hestiastore.index.scarceindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.hestiastore.index.Entry;
import org.junit.jupiter.api.Test;

class ScarceIndexSnapshotTest {

    private static final Comparator<String> COMPARATOR = Comparator
            .naturalOrder();

    @Test
    void test_findSegmentId_returnsMatchingValue() {
        final ScarceIndexSnapshot<String> snapshot = snapshot(List
                .of(Entry.of("bbb", 1), Entry.of("ccc", 2), Entry.of("ddd", 3)));

        assertEquals(1, snapshot.findSegmentId("bbb"));
        assertEquals(2, snapshot.findSegmentId("ccc"));
        assertEquals(3, snapshot.findSegmentId("ddd"));
        assertEquals(3, snapshot.findSegmentId("ccc-suffix"));
    }

    @Test
    void test_findSegmentId_betweenKeys_returnsUpperSegmentId() {
        final ScarceIndexSnapshot<String> snapshot = snapshot(List
                .of(Entry.of("bbb", 1), Entry.of("ddd", 2), Entry.of("fff", 3)));

        assertEquals(2, snapshot.findSegmentId("ccc"));
        assertEquals(3, snapshot.findSegmentId("eee"));
    }

    @Test
    void test_findSegmentId_outOfRange() {
        final ScarceIndexSnapshot<String> snapshot = snapshot(
                List.of(Entry.of("bbb", 1), Entry.of("ccc", 2)));

        assertEquals(1, snapshot.findSegmentId("aaa"));
        assertNull(snapshot.findSegmentId("ddd"));
    }

    @Test
    void test_findSegmentId_emptySnapshot() {
        final ScarceIndexSnapshot<String> snapshot = snapshot(List.of());
        assertNull(snapshot.findSegmentId("aaa"));
    }

    @Test
    void test_findSegmentId_singleEntry() {
        final ScarceIndexSnapshot<String> snapshot = snapshot(
                List.of(Entry.of("bbb", 1)));

        assertEquals(1, snapshot.findSegmentId("aaa"));
        assertEquals(1, snapshot.findSegmentId("bbb"));
        assertNull(snapshot.findSegmentId("ccc"));
    }

    @Test
    void test_findSegmentId_usesProvidedComparator() {
        final ScarceIndexSnapshot<String> snapshot = new ScarceIndexSnapshot<>(
                String.CASE_INSENSITIVE_ORDER,
                List.of(Entry.of("BBB", 1), Entry.of("CCC", 2),
                        Entry.of("DDD", 3)));

        assertEquals(2, snapshot.findSegmentId("ccc"));
        assertEquals(1, snapshot.findSegmentId("aaa"));
        assertNull(snapshot.findSegmentId("zzz"));
    }

    @Test
    void test_getters_emptySnapshot() {
        final ScarceIndexSnapshot<String> snapshot = snapshot(List.of());

        assertEquals(0, snapshot.getKeyCount());
        assertNull(snapshot.getMinKey());
        assertNull(snapshot.getMaxKey());
        assertEquals(0, snapshot.getSegments().count());
    }

    @Test
    void test_constructor_copiesInputEntries() {
        final List<Entry<String, Integer>> source = new ArrayList<>();
        source.add(Entry.of("bbb", 1));
        source.add(Entry.of("ccc", 2));
        final ScarceIndexSnapshot<String> snapshot = snapshot(source);

        source.clear();
        source.add(Entry.of("aaa", 999));

        assertEquals(2, snapshot.getKeyCount());
        assertEquals("bbb", snapshot.getMinKey());
        assertEquals("ccc", snapshot.getMaxKey());
        assertEquals(List.of(Entry.of("bbb", 1), Entry.of("ccc", 2)),
                snapshot.getSegments().toList());
    }

    @Test
    void test_constructor_requiresEntries() {
        assertThrows(IllegalArgumentException.class,
                () -> new ScarceIndexSnapshot<>(COMPARATOR, null));
    }

    @Test
    void test_constructor_requiresComparator() {
        assertThrows(IllegalArgumentException.class,
                () -> new ScarceIndexSnapshot<>(null, List.of()));
    }

    @Test
    void test_findSegmentId_nullKey() {
        final ScarceIndexSnapshot<String> snapshot = snapshot(
                List.of(Entry.of("bbb", 1)));

        assertThrows(IllegalArgumentException.class,
                () -> snapshot.findSegmentId(null));
    }

    private ScarceIndexSnapshot<String> snapshot(
            final List<Entry<String, Integer>> entries) {
        return new ScarceIndexSnapshot<>(COMPARATOR, entries);
    }

}
