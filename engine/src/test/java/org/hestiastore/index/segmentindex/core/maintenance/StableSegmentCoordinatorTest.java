package org.hestiastore.index.segmentindex.core.maintenance;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.core.routing.IndexResult;
import org.hestiastore.index.segmentindex.core.routing.StableSegmentAccess;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.core.routing.BackgroundSplitCoordinator;
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
    private StableSegmentAccess<String, String> stableSegmentGateway;

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
    void invalidateIterators_ignoresLookupFailureForMappedSegment() {
        final SegmentId segmentId = createBootstrapSegment("key");
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenThrow(new org.hestiastore.index.IndexException("boom"));

        assertDoesNotThrow(() -> coordinator.invalidateIterators());
    }

    @Test
    void openIteratorWithRetry_returnsIteratorFromCore() {
        final SegmentId segmentId = createBootstrapSegment("key");
        final EntryIterator<String, String> iterator = EntryIterator
                .make(List.<Entry<String, String>>of().iterator());
        when(stableSegmentGateway.openIterator(segmentId,
                SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(IndexResult.ok(iterator));

        final EntryIterator<String, String> result = coordinator
                .openIteratorWithRetry(segmentId,
                        SegmentIteratorIsolation.FAIL_FAST);

        assertSame(iterator, result);
        verify(stableSegmentGateway).openIterator(segmentId,
                SegmentIteratorIsolation.FAIL_FAST);
    }

    @Test
    void openIteratorWithRetry_retriesBusyAndFailsForError() {
        final SegmentId busySegmentId = createBootstrapSegment("busy-key");
        final EntryIterator<String, String> iterator = EntryIterator
                .make(List.<Entry<String, String>>of().iterator());
        when(stableSegmentGateway.openIterator(busySegmentId,
                SegmentIteratorIsolation.FAIL_FAST))
                        .thenReturn(IndexResult.busy(),
                                IndexResult.ok(iterator));

        assertSame(iterator,
                coordinator.openIteratorWithRetry(busySegmentId,
                        SegmentIteratorIsolation.FAIL_FAST));

        final SegmentId errorSegmentId = createBootstrapSegment("error-key");
        when(stableSegmentGateway.openIterator(errorSegmentId,
                SegmentIteratorIsolation.FAIL_FAST))
                        .thenReturn(IndexResult.error());

        assertThrows(IndexException.class,
                () -> coordinator.openIteratorWithRetry(errorSegmentId,
                        SegmentIteratorIsolation.FAIL_FAST));
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

        assertEquals(1L, stats.getFlushBusyRetryCount());
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

        assertEquals(1L, stats.getCompactBusyRetryCount());
        assertEquals(48L, stats.getCompactAcceptedToReadyP95Micros());
    }

    @Test
    void compactSegment_coalescesBusyOperationWhenWaitingIsDisabled() {
        final SegmentId segmentId = createBootstrapSegment("busy-key");
        when(stableSegmentGateway.compact(segmentId))
                .thenReturn(IndexResult.busy());

        assertDoesNotThrow(() -> coordinator.compactSegment(segmentId, false));
        assertEquals(0L, stats.getCompactBusyRetryCount());
    }

    @Test
    void compactSegment_ignoresUnmappedErrorStatus() {
        final SegmentId segmentId = SegmentId.of(999);
        when(stableSegmentGateway.compact(segmentId))
                .thenReturn(IndexResult.error());

        assertDoesNotThrow(() -> coordinator.compactSegment(segmentId, true));
    }

    @Test
    void flushSegment_throwsForMappedErrorStatus() {
        final SegmentId segmentId = createBootstrapSegment("error-key");
        when(stableSegmentGateway.flush(segmentId))
                .thenReturn(IndexResult.error());

        assertThrows(IndexException.class,
                () -> coordinator.flushSegment(segmentId, true));
    }

    @Test
    void flushSegment_failsWhenAcceptedSegmentEntersErrorState() {
        final SegmentId segmentId = createBootstrapSegment("error-state-key");
        when(segmentHandle.getRuntime()).thenReturn(runtime);
        when(stableSegmentGateway.flush(segmentId))
                .thenReturn(IndexResult.ok(segmentHandle));
        when(runtime.getState()).thenReturn(SegmentState.ERROR);

        assertThrows(IndexException.class,
                () -> coordinator.flushSegment(segmentId, true));
    }

    @Test
    void flushMappedSegmentsAndWait_runsInsidePausedSplitScope() {
        final SegmentId firstSegment = createBootstrapSegment("key-a");
        final SegmentId secondSegment = createBootstrapSegment("key-z");
        when(segmentHandle.getRuntime()).thenReturn(runtime);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(stableSegmentGateway.flush(firstSegment))
                .thenReturn(IndexResult.ok(segmentHandle));
        when(stableSegmentGateway.flush(secondSegment))
                .thenReturn(IndexResult.ok(segmentHandle));
        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(backgroundSplitCoordinator)
                .runWithSplitSchedulingPaused(any(Runnable.class));

        coordinator.flushMappedSegmentsAndWait();

        verify(backgroundSplitCoordinator)
                .runWithSplitSchedulingPaused(any(Runnable.class));
        verify(stableSegmentGateway).flush(firstSegment);
        verify(stableSegmentGateway).flush(secondSegment);
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
