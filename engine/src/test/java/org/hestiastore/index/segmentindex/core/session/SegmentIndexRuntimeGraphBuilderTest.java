package org.hestiastore.index.segmentindex.core.session;

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
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.WalDurabilityMode;
import org.hestiastore.index.segmentindex.core.maintenance.IndexExecutorRegistry;
import org.hestiastore.index.segmentindex.core.session.IndexCloseCoordinator;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.core.routing.IndexOperationTrackingAccess;
import org.hestiastore.index.segmentindex.core.session.state.IndexStateCoordinator;
import org.mockito.Mockito;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SegmentIndexRuntimeGraphBuilderTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    private IndexExecutorRegistry executorRegistry;
    private SegmentIndexRuntime<Integer, String> runtime;
    private org.hestiastore.index.segmentindex.core.session.SegmentIndexImpl<Integer, String> closeOwner;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        final AtomicReference<RuntimeException> failureRef = new AtomicReference<>();
        executorRegistry = new IndexExecutorRegistry(conf);
        closeOwner = Mockito.mock(
                org.hestiastore.index.segmentindex.core.session.SegmentIndexImpl.class);
        runtime = newBuilder(conf, failureRef::set, null).build();
    }

    @AfterEach
    void tearDown() {
        if (runtime != null) {
            new IndexCloseCoordinator<>(logger,
                    "runtime-graph-builder-test",
                    Mockito.mock(IndexStateCoordinator.class),
                    Mockito.mock(IndexOperationTrackingAccess.class), new Stats(),
                    runtime).close();
            SegmentIndexRuntimeTestAccess.keyToSegmentMap(runtime).close();
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
        assertNotNull(SegmentIndexRuntimeTestAccess.controlPlane(runtime));
    }

    @Test
    void buildClosesCreatedResourcesWhenLaterAssemblyFails() {
        final AtomicReference<KeyToSegmentMap<Integer>> keyToSegmentMapRef = new AtomicReference<>();
        final AtomicReference<WalRuntime<Integer, String>> walRuntimeRef = new AtomicReference<>();
        final RuntimeException failure = new IllegalStateException("boom");
        final SegmentIndexRuntimeGraphBuilder<Integer, String> builder = newBuilder(
                buildWalEnabledConf(), ignored -> {
                }, new SegmentIndexRuntimeGraphBuilder.ResourceCreationObserver<>() {
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
                builder::build);

        assertSame(failure, thrown);
        assertTrue(keyToSegmentMapRef.get().wasClosed());
        final WalRuntime<Integer, String> createdWalRuntime = walRuntimeRef
                .get();
        final IndexException walClosedFailure = assertThrows(
                IndexException.class,
                () -> createdWalRuntime.appendPut(1, "one"));
        assertTrue(walClosedFailure.getMessage().contains("already closed"));
    }

    private SegmentIndexRuntimeGraphBuilder<Integer, String> newBuilder(
            final IndexConfiguration<Integer, String> conf,
            final Consumer<RuntimeException> failureHandler,
            final SegmentIndexRuntimeGraphBuilder.ResourceCreationObserver<Integer, String> resourceCreationObserver) {
        final SegmentIndexRuntimeInputs<Integer, String> request =
                new SegmentIndexRuntimeInputs<>(logger,
                        new MemDirectory(), tdi, tds, conf,
                        conf.resolveRuntimeConfiguration(), executorRegistry,
                        new Stats(), new AtomicLong(), new AtomicLong(),
                        new AtomicLong(), () -> SegmentIndexState.READY,
                        failureHandler);
        if (resourceCreationObserver == null) {
            return new SegmentIndexRuntimeGraphBuilder<>(request);
        }
        return new SegmentIndexRuntimeGraphBuilder<>(request,
                resourceCreationObserver);
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(tdi)
                .withValueTypeDescriptor(tds)
                .withName("segment-index-runtime-graph-builder-test")
                .withContextLoggingEnabled(false)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInActivePartition(5)
                .withMaxNumberOfKeysInPartitionBuffer(6)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withMaxNumberOfKeysInSegment(100)
                .withMaxNumberOfSegmentsInCache(3)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSizeInBytes(1024)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }

    private IndexConfiguration<Integer, String> buildWalEnabledConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(tdi)
                .withValueTypeDescriptor(tds)
                .withName("segment-index-runtime-graph-builder-wal-test")
                .withContextLoggingEnabled(false)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInActivePartition(5)
                .withMaxNumberOfKeysInPartitionBuffer(6)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withMaxNumberOfKeysInSegment(100)
                .withMaxNumberOfSegmentsInCache(3)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSizeInBytes(1024)
                .withWal(Wal.builder()
                        .withDurabilityMode(WalDurabilityMode.SYNC)
                        .build())
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }
}
