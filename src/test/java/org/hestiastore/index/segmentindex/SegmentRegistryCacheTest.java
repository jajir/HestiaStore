package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentRegistryCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentRegistryCacheTest {

    @Mock
    private Segment<Integer, String> segmentA;
    @Mock
    private Segment<Integer, String> segmentB;
    @Mock
    private Segment<Integer, String> segmentC;

    private SegmentRegistryCache<Integer, String> cache;

    @BeforeEach
    void setUp() {
        cache = new SegmentRegistryCache<>();
    }

    @AfterEach
    void tearDown() {
        cache = null;
    }

    @Test
    void put_get_remove_tracks_instances() {
        final SegmentId segmentId = SegmentId.of(1);
        cache.withLock(() -> cache.putLocked(segmentId, segmentA));

        assertSame(segmentA, cache.withLock(() -> cache.getLocked(segmentId)));
        assertTrue(cache.withLock(
                () -> cache.isSegmentInstanceLocked(segmentId, segmentA)));
        assertFalse(cache.withLock(
                () -> cache.isSegmentInstanceLocked(segmentId, segmentB)));

        assertSame(segmentA,
                cache.withLock(() -> cache.removeLocked(segmentId)));
        assertNull(cache.withLock(() -> cache.getLocked(segmentId)));
    }

    @Test
    void snapshotAndClearLocked_returns_snapshot_and_clears_cache() {
        final SegmentId firstId = SegmentId.of(1);
        final SegmentId secondId = SegmentId.of(2);
        final List<Segment<Integer, String>> snapshot = cache.withLock(() -> {
            cache.putLocked(firstId, segmentA);
            cache.putLocked(secondId, segmentB);
            return cache.snapshotAndClearLocked();
        });

        assertEquals(List.of(segmentA, segmentB), snapshot);
        assertNull(cache.withLock(() -> cache.getLocked(firstId)));
        assertNull(cache.withLock(() -> cache.getLocked(secondId)));
    }

    @Test
    void needsEvictionLocked_returns_false_when_only_protected() {
        final SegmentId firstId = SegmentId.of(1);
        final SegmentId secondId = SegmentId.of(2);
        final boolean needsEviction = cache.withLock(() -> {
            cache.putLocked(firstId, segmentA);
            cache.putLocked(secondId, segmentB);
            return cache.needsEvictionLocked(1,
                    Set.of(firstId, secondId));
        });

        assertFalse(needsEviction);
    }

    @Test
    void evictIfNeededLocked_removes_lru_unprotected() {
        final SegmentId firstId = SegmentId.of(1);
        final SegmentId secondId = SegmentId.of(2);
        final SegmentId thirdId = SegmentId.of(3);
        final List<Segment<Integer, String>> evicted = new ArrayList<>();

        cache.withLock(() -> {
            cache.putLocked(firstId, segmentA);
            cache.putLocked(secondId, segmentB);
            cache.getLocked(firstId); // refresh LRU order
            cache.putLocked(thirdId, segmentC);
            cache.evictIfNeededLocked(2, Set.of(), evicted);
        });

        assertEquals(List.of(segmentB), evicted);
        assertTrue(cache.withLock(
                () -> cache.isSegmentInstanceLocked(firstId, segmentA)));
        assertTrue(cache.withLock(
                () -> cache.isSegmentInstanceLocked(thirdId, segmentC)));
    }

    @Test
    void evictIfNeededLocked_skips_protected_ids() {
        final SegmentId protectedId = SegmentId.of(1);
        final SegmentId evictedId = SegmentId.of(2);
        final List<Segment<Integer, String>> evicted = new ArrayList<>();

        cache.withLock(() -> {
            cache.putLocked(protectedId, segmentA);
            cache.putLocked(evictedId, segmentB);
            cache.evictIfNeededLocked(1, Set.of(protectedId), evicted);
        });

        assertEquals(List.of(segmentB), evicted);
        assertTrue(cache.withLock(
                () -> cache.isSegmentInstanceLocked(protectedId, segmentA)));
        assertFalse(cache.withLock(
                () -> cache.isSegmentInstanceLocked(evictedId, segmentB)));
    }
}
