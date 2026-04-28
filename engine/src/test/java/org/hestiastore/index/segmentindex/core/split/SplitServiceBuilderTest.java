package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexMaintenanceConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class SplitServiceBuilderTest {

    @Test
    void buildCreatesSplitServiceWithAllViewsBackedBySameRuntime() {
        final ScheduledExecutorService scheduler = Executors
                .newSingleThreadScheduledExecutor();
        try {
            final IndexConfiguration<String, String> conf = mock(
                    IndexConfiguration.class);
            final IndexMaintenanceConfiguration maintenance = mock(
                    IndexMaintenanceConfiguration.class);
            final SegmentRegistry<String, String> segmentRegistry = mock(SegmentRegistry.class);
            when(conf.maintenance()).thenReturn(maintenance);
            when(maintenance.busyBackoffMillis()).thenReturn(1);
            when(maintenance.busyTimeoutMillis()).thenReturn(1);
            when(segmentRegistry.materialization())
                    .thenReturn(mock(SegmentRegistry.Materialization.class));
            final SplitService service = SplitService
                    .<String, String>builder()
                    .conf(conf)
                    .runtimeTuningState(mock(RuntimeTuningState.class))
                    .keyComparator(mockComparator())
                    .keyToSegmentMap(mock(KeyToSegmentMap.class))
                    .segmentTopology(mock(SegmentTopology.class))
                    .segmentRegistry(segmentRegistry)
                    .directoryFacade(mock(Directory.class))
                    .splitExecutor(directExecutor())
                    .workerExecutor(directExecutor())
                    .splitPolicyScheduler(scheduler)
                    .stateSupplier(() -> SegmentIndexState.READY)
                    .failureHandler(ex -> {
                    })
                    .stats(new Stats())
                    .build();

            assertSame(service, service.splitMetricsView());
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void buildRejectsMissingConfiguration() {
        final ScheduledExecutorService scheduler = Executors
                .newSingleThreadScheduledExecutor();
        try {
            final SplitServiceBuilder<String, String> builder = SplitService
                    .<String, String>builder()
                    .runtimeTuningState(mock(RuntimeTuningState.class))
                    .keyComparator(mockComparator())
                    .keyToSegmentMap(mock(KeyToSegmentMap.class))
                    .segmentRegistry(mock(SegmentRegistry.class))
                    .directoryFacade(mock(Directory.class))
                    .splitExecutor(directExecutor())
                    .workerExecutor(directExecutor())
                    .splitPolicyScheduler(scheduler)
                    .stateSupplier(() -> SegmentIndexState.READY)
                    .failureHandler(ex2 -> {
                    })
                    .stats(new Stats());

            final IllegalArgumentException ex = assertThrows(
                    IllegalArgumentException.class, builder::build);

            assertEquals("Property 'conf' must not be null.",
                    ex.getMessage());
        } finally {
            scheduler.shutdownNow();
        }
    }

    private Comparator<String> mockComparator() {
        return mock(Comparator.class);
    }

    private Executor directExecutor() {
        return Runnable::run;
    }
}
