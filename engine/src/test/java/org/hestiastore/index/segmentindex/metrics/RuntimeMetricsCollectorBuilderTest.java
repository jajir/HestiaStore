package org.hestiastore.index.segmentindex.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.core.split.SplitMetricsSnapshot;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RuntimeMetricsCollectorBuilderTest {

    private static final String CONF_PROPERTY_MESSAGE =
            "Property 'conf' must not be null.";

    @Mock
    private KeyToSegmentMap<Integer> keyToSegmentMap;

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    private IndexConfiguration<Integer, String> conf;
    private RuntimeTuningState runtimeTuningState;
    private Stats stats;
    private AtomicLong compactRequestHighWaterMark;
    private AtomicLong flushRequestHighWaterMark;
    private AtomicLong lastAppliedWalLsn;
    private ExecutorRegistry executorRegistry;
    private WalRuntime<Integer, String> walRuntime;

    @BeforeEach
    void setUp() {
        conf = SegmentIndexMetricsTestConfigurationFactory.build(
                "metric-service-builder");
        runtimeTuningState = RuntimeTuningState.fromConfiguration(conf);
        stats = new Stats();
        compactRequestHighWaterMark = new AtomicLong();
        flushRequestHighWaterMark = new AtomicLong();
        lastAppliedWalLsn = new AtomicLong(42L);
        executorRegistry = ExecutorRegistryFixture.from(conf);
        walRuntime = WalRuntime.open(new MemDirectory(), IndexWalConfiguration.EMPTY, null, null);
    }

    @AfterEach
    void tearDown() {
        if (walRuntime != null) {
            walRuntime.close();
        }
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            executorRegistry.close();
        }
    }

    @Test
    void buildCreatesRuntimeMetricsCollectorThatSnapshotsRuntime() {
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of());
        when(segmentRegistry.metricsSnapshot())
                .thenReturn(new SegmentRegistryCacheStats(2L, 3L, 4L, 5L, 6,
                        7));
        stats.recordGetRequest();

        final RuntimeMetricsCollector service = completeBuilder().build();
        final SegmentIndexMetricsSnapshot snapshot = service.metricsSnapshot();

        assertNotNull(service);
        assertEquals(RuntimeMetricsCollectorImpl.class, service.getClass());
        assertEquals(1L, snapshot.getGetOperationCount());
        assertEquals(2L, snapshot.getRegistryCacheHitCount());
        assertEquals(2, snapshot.getSplitInFlightCount());
        assertEquals(1, snapshot.getLegacyPartitionCompatibilityMetrics().getSplitBlockedPartitionCount());
        assertEquals(42L, snapshot.getWalAppliedLsn());
        assertEquals(SegmentIndexState.READY, snapshot.getState());
    }

    @Test
    void buildRejectsMissingConfiguration() {
        final RuntimeMetricsCollectorBuilder<Integer, String> builder =
                completeBuilder().withConf(null);

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, builder::build);

        assertEquals(CONF_PROPERTY_MESSAGE, ex.getMessage());
    }

    private RuntimeMetricsCollectorBuilder<Integer, String> completeBuilder() {
        return RuntimeMetricsCollector.<Integer, String>builder()
                .withConf(conf)
                .withKeyToSegmentMap(keyToSegmentMap)
                .withSegmentRegistry(segmentRegistry)
                .withSplitSnapshotSupplier(
                        () -> new SplitMetricsSnapshot(2, 1))
                .withExecutorRegistry(executorRegistry)
                .withRuntimeTuningState(runtimeTuningState)
                .withWalRuntime(walRuntime)
                .withStats(stats)
                .withCompactRequestHighWaterMark(compactRequestHighWaterMark)
                .withFlushRequestHighWaterMark(flushRequestHighWaterMark)
                .withLastAppliedWalLsn(lastAppliedWalLsn)
                .withStateSupplier(() -> SegmentIndexState.READY);
    }
}
