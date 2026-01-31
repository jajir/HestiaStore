package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentHandler;
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
        final SegmentHandler<Integer, String> handlerA = new SegmentHandler<>(
                segmentA);
        cache.withLock(() -> cache.putLocked(segmentId, handlerA));

        assertSame(handlerA, cache.withLock(() -> cache.getLocked(segmentId)));
        assertTrue(cache.withLock(
                () -> cache.isSegmentInstanceLocked(segmentId, segmentA)));
        assertFalse(cache.withLock(
                () -> cache.isSegmentInstanceLocked(segmentId, segmentB)));

        assertSame(handlerA,
                cache.withLock(() -> cache.removeLocked(segmentId)));
        assertNull(cache.withLock(() -> cache.getLocked(segmentId)));
    }

    @Test
    void snapshotAndClearLocked_returns_snapshot_and_clears_cache() {
        final SegmentId firstId = SegmentId.of(1);
        final SegmentId secondId = SegmentId.of(2);
        final SegmentHandler<Integer, String> handlerA = new SegmentHandler<>(
                segmentA);
        final SegmentHandler<Integer, String> handlerB = new SegmentHandler<>(
                segmentB);
        final List<Segment<Integer, String>> snapshot = cache.withLock(() -> {
            cache.putLocked(firstId, handlerA);
            cache.putLocked(secondId, handlerB);
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
        final SegmentHandler<Integer, String> handlerA = new SegmentHandler<>(
                segmentA);
        final SegmentHandler<Integer, String> handlerB = new SegmentHandler<>(
                segmentB);
        handlerA.lock();
        handlerB.lock();
        final boolean needsEviction = cache.withLock(() -> {
            cache.putLocked(firstId, handlerA);
            cache.putLocked(secondId, handlerB);
            return cache.needsEvictionLocked(1);
        });

        assertFalse(needsEviction);
    }

    @Test
    void evictIfNeededLocked_removes_lru_unprotected() {
        final SegmentId firstId = SegmentId.of(1);
        final SegmentId secondId = SegmentId.of(2);
        final SegmentId thirdId = SegmentId.of(3);
        final List<Segment<Integer, String>> evicted = new ArrayList<>();
        final SegmentHandler<Integer, String> handlerA = new SegmentHandler<>(
                segmentA);
        final SegmentHandler<Integer, String> handlerB = new SegmentHandler<>(
                segmentB);
        final SegmentHandler<Integer, String> handlerC = new SegmentHandler<>(
                segmentC);

        cache.withLock(() -> {
            cache.putLocked(firstId, handlerA);
            cache.putLocked(secondId, handlerB);
            cache.getLocked(firstId); // refresh LRU order
            cache.putLocked(thirdId, handlerC);
            cache.evictIfNeededLocked(2, evicted);
        });

        assertEquals(List.of(segmentB), evicted);
        assertTrue(cache.withLock(
                () -> cache.isSegmentInstanceLocked(firstId, segmentA)));
        assertTrue(cache.withLock(
                () -> cache.isSegmentInstanceLocked(thirdId, segmentC)));
    }

    @Test
    void evictIfNeededLocked_skips_protected_ids() {
        final SegmentId lockedId = SegmentId.of(1);
        final SegmentId evictedId = SegmentId.of(2);
        final List<Segment<Integer, String>> evicted = new ArrayList<>();
        final SegmentHandler<Integer, String> lockedHandler = new SegmentHandler<>(
                segmentA);
        final SegmentHandler<Integer, String> evictedHandler = new SegmentHandler<>(
                segmentB);
        lockedHandler.lock();

        cache.withLock(() -> {
            cache.putLocked(lockedId, lockedHandler);
            cache.putLocked(evictedId, evictedHandler);
            cache.evictIfNeededLocked(1, evicted);
        });

        assertEquals(List.of(segmentB), evicted);
        assertTrue(cache.withLock(
                () -> cache.isSegmentInstanceLocked(lockedId, segmentA)));
        assertFalse(cache.withLock(
                () -> cache.isSegmentInstanceLocked(evictedId, segmentB)));
    }
}
