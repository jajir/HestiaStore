package org.hestiastore.index.scarceindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    void test_findSegmentId_outOfRange() {
        final ScarceIndexSnapshot<String> snapshot = snapshot(
                List.of(Entry.of("bbb", 1), Entry.of("ccc", 2)));

        assertEquals(1, snapshot.findSegmentId("aaa"));
        assertNull(snapshot.findSegmentId("ddd"));
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
    void test_constructor_requiresEntries() {
        assertThrows(IllegalArgumentException.class,
                () -> new ScarceIndexSnapshot<>(COMPARATOR, null));
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
