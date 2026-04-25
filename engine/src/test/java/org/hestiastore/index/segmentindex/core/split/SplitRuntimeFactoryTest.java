package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.Snapshot;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;

class SplitRuntimeFactoryTest {

    @Test
    void createReturnsSplitServiceBuiltAroundServiceViews() {
        final ScheduledExecutorService scheduler = Executors
                .newSingleThreadScheduledExecutor();
        try {
            final IndexConfiguration<String, String> conf = mock(
                    IndexConfiguration.class);
            final SegmentRegistry<String, String> segmentRegistry =
                    mock(SegmentRegistry.class);
            when(conf.getIndexBusyBackoffMillis()).thenReturn(1);
            when(conf.getIndexBusyTimeoutMillis()).thenReturn(1);
            when(segmentRegistry.materialization())
                    .thenReturn(mock(SegmentRegistry.Materialization.class));
            final KeyToSegmentMap<String> keyToSegmentMap = mock(
                    KeyToSegmentMap.class);
            final Snapshot<String> snapshot = mock(Snapshot.class);
            when(snapshot.getSegmentIds(any())).thenReturn(List.of());
            when(snapshot.version()).thenReturn(0L);
            when(keyToSegmentMap.snapshot()).thenReturn(snapshot);
            final SplitService<String, String> service =
                    SplitRuntimeFactory.create(
                            conf,
                            mock(RuntimeTuningState.class),
                            mockComparator(),
                            keyToSegmentMap,
                            segmentRegistry,
                            mock(Directory.class),
                            directExecutor(),
                            directExecutor(),
                            scheduler,
                            () -> SegmentIndexState.READY,
                            ex -> {
                            },
                            new Stats());

            assertSame(service, service.splitMetricsView());
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void helperMethodsDelegateToSplitServiceViews() {
        final SplitService<String, String> service = mock(SplitService.class);
        final SplitMetricsView metricsView = mock(SplitMetricsView.class);
        final SplitMetricsSnapshot snapshot =
                new SplitMetricsSnapshot(3, 4);
        when(service.splitMetricsView()).thenReturn(metricsView);
        when(metricsView.metricsSnapshot()).thenReturn(snapshot);

        SplitRuntimeFactory.requestFullSplitScan(service);
        assertSame(snapshot, SplitRuntimeFactory.metricsSnapshot(service));

        verify(service).requestFullSplitScan();
    }

    @SuppressWarnings("unchecked")
    private Comparator<String> mockComparator() {
        return mock(Comparator.class);
    }

    private Executor directExecutor() {
        return Runnable::run;
    }
}
