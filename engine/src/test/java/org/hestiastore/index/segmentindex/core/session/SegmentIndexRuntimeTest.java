package org.hestiastore.index.segmentindex.core.session;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.split.SplitStatsRecorder;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexRuntimeStorage;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexRuntimeTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    private ExecutorRegistry executorRegistry;
    private SegmentIndexRuntime<Integer, String> runtime;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        executorRegistry = ExecutorRegistryFixture.from(conf);
        final AtomicReference<RuntimeException> failureRef = new AtomicReference<>();
        runtime = new SegmentIndexRuntimeFactory<>(
                new SegmentIndexRuntimeOpenContext<>(
                        new MemDirectory(), tdi, tds, effective(conf),
                        executorRegistry, new IndexOperationStatsRecorder(),
                        new MaintenanceStatsRecorder(),
                        new SplitStatsRecorder(), new AtomicLong(),
                        new AtomicLong(), new AtomicLong(),
                        () -> SegmentIndexState.READY, failureRef::set))
                .open();
    }

    @AfterEach
    void tearDown() {
        if (runtime != null) {
            final SegmentIndexStateMachine stateMachine = new SegmentIndexStateMachine();
            stateMachine.markReady();
            new IndexCloseCoordinator<>("runtime-test", stateMachine,
                    mock(SegmentIndexOperationGate.class),
                    new IndexOperationStatsRecorder(),
                    runtime, executorRegistry,
                    new IndexDirectoryLock(new MemDirectory())).close();
        }
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            executorRegistry.close();
        }
    }

    @Test
    void openBuildsAllCoreCollaborators() {
        assertNotNull(SegmentIndexRuntimeTestAccess.keyToSegmentMap(runtime));
        assertNotNull(SegmentIndexRuntimeTestAccess.segmentRegistry(runtime));
        assertNotNull(
                SegmentIndexRuntimeTestAccess.runtimeTuningState(runtime));
        assertNotNull(SegmentIndexRuntimeTestAccess.walRuntime(runtime));
        assertNotNull(SegmentIndexRuntimeTestAccess.walCoordinator(runtime));
        assertNotNull(SegmentIndexRuntimeTestAccess.operationAccess(runtime));
        assertNotNull(
                SegmentIndexRuntimeTestAccess.metricsSnapshotSupplier(runtime));
        assertNotNull(SegmentIndexRuntimeTestAccess.runtimeTuning(runtime));
    }

    @Test
    @SuppressWarnings("unchecked")
    void closeAfterFailedInitializationClosesRuntimeResourcesInOrder() {
        final SegmentTopologyRuntimeAccess<Integer, String> topologyRuntime =
                mock(SegmentTopologyRuntimeAccess.class);
        final KeyToSegmentMap<Integer> keyToSegmentMap =
                mock(KeyToSegmentMap.class);
        final SegmentRegistry<Integer, String> segmentRegistry =
                mock(SegmentRegistry.class);
        final WalRuntime<Integer, String> walRuntime = mock(WalRuntime.class);
        when(keyToSegmentMap.wasClosed()).thenReturn(false);
        final SegmentIndexRuntime<Integer, String> failedRuntime =
                newRuntime(topologyRuntime, keyToSegmentMap, segmentRegistry,
                        walRuntime);

        failedRuntime.closeAfterFailedInitialization();

        final InOrder inOrder = inOrder(topologyRuntime, segmentRegistry,
                keyToSegmentMap, walRuntime);
        inOrder.verify(topologyRuntime).closeSplitRuntime();
        inOrder.verify(segmentRegistry).close();
        inOrder.verify(keyToSegmentMap).wasClosed();
        inOrder.verify(keyToSegmentMap).close();
        inOrder.verify(walRuntime).close();
    }

    @SuppressWarnings("unchecked")
    private SegmentIndexRuntime<Integer, String> newRuntime(
            final SegmentTopologyRuntimeAccess<Integer, String> topologyRuntime,
            final KeyToSegmentMap<Integer> keyToSegmentMap,
            final SegmentRegistry<Integer, String> segmentRegistry,
            final WalRuntime<Integer, String> walRuntime) {
        return new SegmentIndexRuntime<>(tdi,
                new SegmentIndexRuntimeStorage<>(
                        mock(RuntimeTuningState.class), keyToSegmentMap,
                        segmentRegistry, mock(IndexRetryPolicy.class)),
                topologyRuntime, walRuntime,
                mock(SegmentIndexRuntimeServices.class));
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(tdi))
                .identity(identity -> identity.valueTypeDescriptor(tds))
                .identity(identity -> identity.name("segment-index-runtime-test"))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }

}
