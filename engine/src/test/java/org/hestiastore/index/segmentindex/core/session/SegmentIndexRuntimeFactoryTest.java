package org.hestiastore.index.segmentindex.core.session;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.user.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.configuration.user.WalDurabilityMode;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.split.SplitStatsRecorder;
import org.mockito.Mockito;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentIndexRuntimeFactoryTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    private ExecutorRegistry executorRegistry;
    private SegmentIndexRuntime<Integer, String> runtime;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        final AtomicReference<RuntimeException> failureRef = new AtomicReference<>();
        executorRegistry = ExecutorRegistryFixture.from(conf);
        runtime = newBuilder(conf, failureRef::set, null).open();
    }

    @AfterEach
    void tearDown() {
        if (runtime != null) {
            final SegmentIndexStateMachine stateMachine = new SegmentIndexStateMachine();
            stateMachine.markReady();
            new IndexCloseCoordinator<>("runtime-graph-builder-test",
                    stateMachine,
                    Mockito.mock(IndexOperationTrackingAccess.class),
                    new IndexOperationStatsRecorder(),
                    runtime, new IndexDirectoryLock(new MemDirectory())).close();
        }
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            executorRegistry.close();
        }
    }

    @Test
    void buildCreatesAllCoreCollaborators() {
        assertNotNull(SegmentIndexRuntimeTestAccess.keyToSegmentMap(runtime));
        assertNotNull(SegmentIndexRuntimeTestAccess.segmentRegistry(runtime));
        assertNotNull(
                SegmentIndexRuntimeTestAccess.runtimeTuningState(runtime));
        assertNotNull(SegmentIndexRuntimeTestAccess.walCoordinator(runtime));
        assertNotNull(SegmentIndexRuntimeTestAccess.operationAccess(runtime));
        assertNotNull(SegmentIndexRuntimeTestAccess.runtimeTuning(runtime));
    }

    @Test
    void buildClosesCreatedResourcesWhenLaterAssemblyFails() {
        final AtomicReference<KeyToSegmentMap<Integer>> keyToSegmentMapRef = new AtomicReference<>();
        final AtomicReference<WalRuntime<Integer, String>> walRuntimeRef = new AtomicReference<>();
        final RuntimeException failure = new IllegalStateException("boom");
        final SegmentIndexRuntimeFactory<Integer, String> builder = newBuilder(
                buildWalEnabledConf(), ignored -> {
                }, new SegmentIndexRuntimeFactory.ResourceCreationObserver<>() {
                    @Override
                    public void onKeyToSegmentMapCreated(
                            final KeyToSegmentMap<Integer> keyToSegmentMap) {
                        keyToSegmentMapRef.set(keyToSegmentMap);
                    }

                    @Override
                    public void onWalRuntimeCreated(
                            final WalRuntime<Integer, String> walRuntime) {
                        walRuntimeRef.set(walRuntime);
                        throw failure;
                    }
                });

        final RuntimeException thrown = assertThrows(RuntimeException.class,
                builder::open);

        assertSame(failure, thrown);
        assertTrue(keyToSegmentMapRef.get().wasClosed());
        final WalRuntime<Integer, String> createdWalRuntime = walRuntimeRef
                .get();
        final IndexException walClosedFailure = assertThrows(
                IndexException.class,
                () -> createdWalRuntime.appendPut(1, "one"));
        assertTrue(walClosedFailure.getMessage().contains("already closed"));
    }

    private SegmentIndexRuntimeFactory<Integer, String> newBuilder(
            final IndexConfiguration<Integer, String> conf,
            final Consumer<RuntimeException> failureHandler,
            final SegmentIndexRuntimeFactory.ResourceCreationObserver<Integer, String> resourceCreationObserver) {
        final SegmentIndexRuntimeOpenContext<Integer, String> request = new SegmentIndexRuntimeOpenContext<>(
                new MemDirectory(), tdi, tds, effective(conf),
                executorRegistry, new IndexOperationStatsRecorder(),
                new MaintenanceStatsRecorder(), new SplitStatsRecorder(),
                new AtomicLong(),
                new AtomicLong(), new AtomicLong(),
                () -> SegmentIndexState.READY, failureHandler);
        if (resourceCreationObserver == null) {
            return new SegmentIndexRuntimeFactory<>(request);
        }
        return new SegmentIndexRuntimeFactory<>(request,
                resourceCreationObserver);
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(tdi))
                .identity(identity -> identity.valueTypeDescriptor(tds))
                .identity(identity -> identity.name("segment-index-runtime-graph-builder-test"))
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

    private IndexConfiguration<Integer, String> buildWalEnabledConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(tdi))
                .identity(identity -> identity.valueTypeDescriptor(tds))
                .identity(identity -> identity.name("segment-index-runtime-graph-builder-wal-test"))
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
                .wal(wal -> wal.configuration(IndexWalConfiguration.builder()
                        .durability(WalDurabilityMode.SYNC)
                        .build()))
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }
}
