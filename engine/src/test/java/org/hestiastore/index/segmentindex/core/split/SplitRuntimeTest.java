package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexMaintenanceConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.SegmentIndexRuntimeState;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLeaseService;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class SplitRuntimeTest {

    @Test
    void createBuildsSplitServiceWithAllViewsBackedBySameRuntime() {
        final ScheduledExecutorService scheduler = Executors
                .newSingleThreadScheduledExecutor();
        try {
            final EffectiveIndexConfiguration<String, String> conf = mock(
                    EffectiveIndexConfiguration.class);
            final EffectiveIndexMaintenanceConfiguration maintenance = mock(
                    EffectiveIndexMaintenanceConfiguration.class);
            final SegmentRegistry<String, String> segmentRegistry = mock(
                    SegmentRegistry.class);
            when(conf.maintenance()).thenReturn(maintenance);
            when(maintenance.busyBackoffMillis()).thenReturn(1);
            when(maintenance.busyTimeoutMillis()).thenReturn(1);
            when(segmentRegistry.materialization())
                    .thenReturn(mock(SegmentRegistry.Materialization.class));

            final SplitRuntime<String, String> service = SplitRuntime.create(
                    conf, mock(RuntimeTuningState.class),
                    mock(SegmentRouteMap.class),
                    mock(MappedSegmentLeaseService.class), segmentRegistry,
                    mock(Directory.class), directExecutor(),
                    directExecutor(), scheduler, runtimeState(),
                    new SplitStatsRecorder());

            assertNotNull(service.statsSnapshot());
        } finally {
            scheduler.shutdownNow();
        }
    }

    private Executor directExecutor() {
        return Runnable::run;
    }

    private SegmentIndexRuntimeState runtimeState() {
        return new SegmentIndexRuntimeState() {

            @Override
            public SegmentIndexState currentState() {
                return SegmentIndexState.READY;
            }

            @Override
            public void markRuntimeFailure(final RuntimeException failure) {
            }
        };
    }
}
