package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.maintenance.SplitMaintenanceSynchronization;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.core.routing.SplitAdmissionAccess;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
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
            final SplitService<String, String> service =
                    SplitRuntimeFactory.create(
                            conf,
                            mock(RuntimeTuningState.class),
                            mockComparator(),
                            mock(KeyToSegmentMap.class),
                            segmentRegistry,
                            mock(Directory.class),
                            directExecutor(),
                            directExecutor(),
                            scheduler,
                            () -> SegmentIndexState.READY,
                            ex -> {
                            },
                            new Stats());

            assertSame(service, SplitRuntimeFactory.admissionAccess(service));
            assertSame(service,
                    SplitRuntimeFactory.maintenanceSynchronization(service));
            assertSame(service, service.splitMetricsView());
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void helperMethodsDelegateToSplitServiceViews() {
        final SplitService<String, String> service = mock(SplitService.class);
        final SplitAdmissionAccess<String, String> admission = mock(
                SplitAdmissionAccess.class);
        final SplitMaintenanceSynchronization<String, String> maintenance =
                mock(SplitMaintenanceSynchronization.class);
        final SplitMetricsView metricsView = mock(SplitMetricsView.class);
        final SplitMetricsSnapshot snapshot =
                new SplitMetricsSnapshot(3, 4);
        when(service.splitAdmission()).thenReturn(admission);
        when(service.splitMaintenance()).thenReturn(maintenance);
        when(service.splitMetricsView()).thenReturn(metricsView);
        when(metricsView.metricsSnapshot()).thenReturn(snapshot);

        assertSame(admission, SplitRuntimeFactory.admissionAccess(service));
        assertSame(maintenance,
                SplitRuntimeFactory.maintenanceSynchronization(service));
        SplitRuntimeFactory.requestReconciliation(service);
        assertSame(snapshot, SplitRuntimeFactory.metricsSnapshot(service));

        verify(maintenance).requestReconciliation();
    }

    @SuppressWarnings("unchecked")
    private Comparator<String> mockComparator() {
        return mock(Comparator.class);
    }

    private Executor directExecutor() {
        return Runnable::run;
    }
}
