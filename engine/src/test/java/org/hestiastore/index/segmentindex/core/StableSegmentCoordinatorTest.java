package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.split.BackgroundSplitCoordinator;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
class StableSegmentCoordinatorTest {

    @Mock
    private SegmentRegistry<String, String> segmentRegistry;

    @Mock
    private BackgroundSplitCoordinator<String, String> backgroundSplitCoordinator;

    @Mock
    private StableSegmentGateway<String, String> stableSegmentGateway;

    @Mock
    private SegmentHandle<String, String> segmentHandle;

    @Mock
    private SegmentHandle.Runtime runtime;

    private Directory directory;
    private KeyToSegmentMapImpl<String> keyToSegmentMap;
    private KeyToSegmentMap<String> synchronizedKeyToSegmentMap;
    private StableSegmentCoordinator<String, String> coordinator;
    private Stats stats;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        keyToSegmentMap = new KeyToSegmentMapImpl<>(directory,
                new TypeDescriptorShortString());
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
        stats = new Stats();
        coordinator = new StableSegmentCoordinator<>(
                LoggerFactory.getLogger(StableSegmentCoordinatorTest.class),
                synchronizedKeyToSegmentMap, segmentRegistry,
                backgroundSplitCoordinator, stableSegmentGateway,
                new IndexRetryPolicy(1, 10), stats);
    }

    @AfterEach
    void tearDown() {
        if (synchronizedKeyToSegmentMap != null
                && !synchronizedKeyToSegmentMap.wasClosed()) {
            synchronizedKeyToSegmentMap.close();
        }
    }

    @Test
    void putEntryForDrain_writesToLoadedSegment() {
        final SegmentId segmentId = createBootstrapSegment("key");
        when(segmentRegistry.loadSegment(segmentId)).thenReturn(segmentHandle);

        assertDoesNotThrow(
                () -> coordinator.putEntryForDrain(segmentId, "key", "value"));
        verify(segmentHandle).put("key", "value");
    }

    @Test
    void invalidateIterators_invalidatesLoadedMappedSegments() {
        final SegmentId segmentId = createBootstrapSegment("key");
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));

        coordinator.invalidateIterators();

        verify(segmentHandle).invalidateIterators();
    }

    @Test
    void openIteratorWithRetry_returnsIteratorFromCore() {
        final SegmentId segmentId = createBootstrapSegment("key");
        final EntryIterator<String, String> iterator = EntryIterator
                .make(List.<Entry<String, String>>of().iterator());
        when(stableSegmentGateway.openIterator(segmentId,
                SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(IndexResult.ok(iterator));

        coordinator.openIteratorWithRetry(segmentId,
                SegmentIteratorIsolation.FAIL_FAST);

        verify(stableSegmentGateway).openIterator(segmentId,
                SegmentIteratorIsolation.FAIL_FAST);
    }

    @Test
    void flushSegment_recordsAcceptedToReadyLatencyAndBusyRetryCount() {
        final SegmentId segmentId = createBootstrapSegment("key");
        coordinator = new StableSegmentCoordinator<>(
                LoggerFactory.getLogger(StableSegmentCoordinatorTest.class),
                synchronizedKeyToSegmentMap, segmentRegistry,
                backgroundSplitCoordinator, stableSegmentGateway,
                new IndexRetryPolicy(1, 10), stats,
                sequenceNanoTimeSupplier(10_000L, 35_000L));
        when(segmentHandle.getRuntime()).thenReturn(runtime);
        when(stableSegmentGateway.flush(segmentId)).thenReturn(
                IndexResult.busy(), IndexResult.ok(segmentHandle));
        when(runtime.getState()).thenReturn(SegmentState.READY);

        coordinator.flushSegment(segmentId, true);

        assertEquals(1L, stats.getFlushBusyRetryCx());
        assertEquals(25L, stats.getFlushAcceptedToReadyP95Micros());
    }

    @Test
    void compactSegment_recordsAcceptedToReadyLatencyAndBusyRetryCount() {
        final SegmentId segmentId = createBootstrapSegment("key");
        coordinator = new StableSegmentCoordinator<>(
                LoggerFactory.getLogger(StableSegmentCoordinatorTest.class),
                synchronizedKeyToSegmentMap, segmentRegistry,
                backgroundSplitCoordinator, stableSegmentGateway,
                new IndexRetryPolicy(1, 10), stats,
                sequenceNanoTimeSupplier(20_000L, 68_000L));
        when(segmentHandle.getRuntime()).thenReturn(runtime);
        when(stableSegmentGateway.compact(segmentId)).thenReturn(
                IndexResult.busy(), IndexResult.ok(segmentHandle));
        when(runtime.getState()).thenReturn(SegmentState.READY);

        coordinator.compactSegment(segmentId, true);

        assertEquals(1L, stats.getCompactBusyRetryCx());
        assertEquals(48L, stats.getCompactAcceptedToReadyP95Micros());
    }

    private static LongSupplier sequenceNanoTimeSupplier(
            final long... nanos) {
        final AtomicInteger index = new AtomicInteger();
        return () -> {
            final int current = index.getAndIncrement();
            final int safeIndex = Math.min(current, nanos.length - 1);
            return nanos[safeIndex];
        };
    }

    private SegmentId createBootstrapSegment(final String key) {
        synchronizedKeyToSegmentMap.extendMaxKeyIfNeeded(key);
        return synchronizedKeyToSegmentMap.findSegmentIdForKey(key);
    }
}
