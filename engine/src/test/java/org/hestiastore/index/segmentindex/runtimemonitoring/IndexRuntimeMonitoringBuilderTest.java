package org.hestiastore.index.segmentindex.runtimemonitoring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.chunkstorecache.LruChunkStoreCache;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.split.SplitStats;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.IndexRuntimeSnapshot;
import org.hestiastore.index.segmentindex.wal.WalMonitoringView;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexRuntimeMonitoringBuilderTest {

    private static final String CONF_PROPERTY_MESSAGE =
            "Property 'conf' must not be null.";

    @Mock
    private KeyToSegmentMap<Integer> keyToSegmentMap;

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    private EffectiveIndexConfiguration<Integer, String> conf;
    private RuntimeTuningState runtimeTuningState;
    private IndexOperationStatsRecorder operationStatsRecorder;
    private MaintenanceStatsRecorder maintenanceStatsRecorder;
    private AtomicLong compactRequestHighWaterMark;
    private AtomicLong flushRequestHighWaterMark;
    private AtomicLong lastAppliedWalLsn;
    private ExecutorRegistry executorRegistry;

    @BeforeEach
    void setUp() {
        conf = SegmentIndexMetricsTestConfigurationFactory.build(
                "metric-service-builder");
        runtimeTuningState = RuntimeTuningState.fromConfiguration(conf);
        operationStatsRecorder = new IndexOperationStatsRecorder();
        maintenanceStatsRecorder = new MaintenanceStatsRecorder();
        compactRequestHighWaterMark = new AtomicLong();
        flushRequestHighWaterMark = new AtomicLong();
        lastAppliedWalLsn = new AtomicLong(42L);
        executorRegistry = ExecutorRegistryFixture.from(conf);
    }

    @AfterEach
    void tearDown() {
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            executorRegistry.close();
        }
    }

    @Test
    void buildCreatesRuntimeMonitoringThatSnapshotsRuntime() {
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of());
        when(segmentRegistry.metricsSnapshot())
                .thenReturn(new SegmentRegistryCacheStats(2L, 3L, 4L, 5L, 6,
                        7));
        operationStatsRecorder.recordGetRequest();

        final IndexRuntimeMonitoring service = completeBuilder().build();
        final IndexRuntimeSnapshot snapshot = service.snapshot();

        assertNotNull(service);
        assertEquals(1L, snapshot.operations().readOperationCount());
        assertEquals(2L, snapshot.registryCache().hitCount());
        assertEquals(2, snapshot.split().inFlightCount());
        assertEquals(42L, snapshot.wal().appliedLsn());
        assertEquals(SegmentIndexState.READY, snapshot.state());
    }

    @Test
    void buildRejectsMissingConfiguration() {
        final IndexRuntimeMonitoringBuilder<Integer, String> builder =
                completeBuilder().withConf(null);

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, builder::build);

        assertEquals(CONF_PROPERTY_MESSAGE, ex.getMessage());
    }

    private IndexRuntimeMonitoringBuilder<Integer, String> completeBuilder() {
        return IndexRuntimeMonitoring.<Integer, String>builder()
                .withConf(conf)
                .withKeyToSegmentMap(keyToSegmentMap)
                .withSegmentRegistry(segmentRegistry)
                .withSplitStatsView(
                        () -> new SplitStats(0L, 2, 1, 0L, 0L))
                .withExecutorRegistry(executorRegistry)
                .withRuntimeTuningState(runtimeTuningState)
                .withChunkStoreCache(new LruChunkStoreCache<>(0))
                .withWalMonitoringView(WalMonitoringView.empty())
                .withIndexOperationStatsRecorder(operationStatsRecorder)
                .withMaintenanceStatsRecorder(maintenanceStatsRecorder)
                .withCompactRequestHighWaterMark(compactRequestHighWaterMark)
                .withFlushRequestHighWaterMark(flushRequestHighWaterMark)
                .withLastAppliedWalLsn(lastAppliedWalLsn)
                .withStateView(readyStateView());
    }

    private static SegmentIndexStateMachine readyStateView() {
        final SegmentIndexStateMachine stateView = new SegmentIndexStateMachine();
        stateView.markReady();
        return stateView;
    }
}
