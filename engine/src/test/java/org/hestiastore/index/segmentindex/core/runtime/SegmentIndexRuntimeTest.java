package org.hestiastore.index.segmentindex.core.runtime;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.lifecycle.IndexCloseCoordinator;
import org.hestiastore.index.segmentindex.core.infrastructure.IndexExecutorRegistry;
import org.hestiastore.index.segmentindex.core.observability.Stats;
import org.hestiastore.index.segmentindex.core.operation.IndexOperationTrackingAccess;
import org.hestiastore.index.segmentindex.core.state.IndexStateCoordinator;
import org.mockito.Mockito;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SegmentIndexRuntimeTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    private IndexExecutorRegistry executorRegistry;
    private SegmentIndexRuntime<Integer, String> runtime;
    private org.hestiastore.index.segmentindex.core.SegmentIndexImpl<Integer, String> closeOwner;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        executorRegistry = new IndexExecutorRegistry(conf);
        final AtomicReference<RuntimeException> failureRef = new AtomicReference<>();
        closeOwner = Mockito.mock(
                org.hestiastore.index.segmentindex.core.SegmentIndexImpl.class);
        runtime = new SegmentIndexRuntimeGraphBuilder<>(
                new SegmentIndexRuntimeInputs<>(
                        logger, new MemDirectory(), tdi, tds, conf,
                        conf.resolveRuntimeConfiguration(), executorRegistry,
                        new Stats(), new AtomicLong(), new AtomicLong(),
                        new AtomicLong(),
                        () -> SegmentIndexState.READY, failureRef::set))
                                .build();
    }

    @AfterEach
    void tearDown() {
        if (runtime != null) {
            new IndexCloseCoordinator<>(logger, "runtime-test",
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
        assertNotNull(SegmentIndexRuntimeTestAccess.controlPlane(runtime));
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(tdi)
                .withValueTypeDescriptor(tds)
                .withName("segment-index-runtime-test")
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

}
