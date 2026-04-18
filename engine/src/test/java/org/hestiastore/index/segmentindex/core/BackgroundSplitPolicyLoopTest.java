package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.split.BackgroundSplitCoordinator;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BackgroundSplitPolicyLoopTest {

    @Mock
    private IndexConfiguration<String, String> conf;

    @Mock
    private RuntimeTuningState runtimeTuningState;

    @Mock
    private KeyToSegmentMapImpl<String> keyToSegmentMap;

    @Mock
    private SegmentRegistry<String, String> segmentRegistry;

    @Mock
    private BackgroundSplitCoordinator<String, String> backgroundSplitCoordinator;

    @Mock
    private ScheduledExecutorService splitPolicyScheduler;

    private KeyToSegmentMap<String> synchronizedKeyToSegmentMap;

    @BeforeEach
    void setUp() {
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
    }

    @Test
    void awaitExhaustedClearsPendingRequestsWhenPolicyDisabled() {
        when(conf.isBackgroundMaintenanceAutoEnabled()).thenReturn(Boolean.TRUE);
        when(conf.getIndexBusyTimeoutMillis()).thenReturn(50);
        when(backgroundSplitCoordinator.splitInFlightCount()).thenReturn(0);
        final AtomicReference<SegmentIndexState> state = new AtomicReference<>(
                SegmentIndexState.CLOSING);
        final BackgroundSplitPolicyLoop<String, String> loop = new BackgroundSplitPolicyLoop<>(
                conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                segmentRegistry, backgroundSplitCoordinator,
                directExecutor(), splitPolicyScheduler, new Stats(),
                state::get, () -> {
                }, failure -> {
                });
        setAtomicBoolean(loop, "backgroundSplitScanRequested", true);
        putHintedSegment(loop, SegmentId.of(7));

        loop.awaitExhausted();

        assertFalse(readAtomicBoolean(loop, "backgroundSplitScanRequested"));
        assertFalse(readHintedSegments(loop).containsKey(SegmentId.of(7)));
    }

    private Executor directExecutor() {
        return Runnable::run;
    }

    private static void setAtomicBoolean(final Object target,
            final String fieldName, final boolean value) {
        readAtomicBooleanField(target, fieldName).set(value);
    }

    private static boolean readAtomicBoolean(final Object target,
            final String fieldName) {
        return readAtomicBooleanField(target, fieldName).get();
    }

    private static AtomicBoolean readAtomicBooleanField(final Object target,
            final String fieldName) {
        try {
            final Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (AtomicBoolean) field.get(target);
        } catch (final ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<SegmentId, Boolean> readHintedSegments(
            final Object target) {
        try {
            final Field field = target.getClass()
                    .getDeclaredField("backgroundSplitHintedSegments");
            field.setAccessible(true);
            return (Map<SegmentId, Boolean>) field.get(target);
        } catch (final ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static void putHintedSegment(final Object target,
            final SegmentId segmentId) {
        readHintedSegments(target).put(segmentId, Boolean.TRUE);
    }
}
