package org.hestiastore.index.segmentindex.core.runtime;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.infrastructure.IndexExecutorRegistry;
import org.hestiastore.index.segmentindex.core.observability.Stats;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class SegmentIndexRuntimeServicesFactoryTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    private IndexExecutorRegistry executorRegistry;
    private SegmentIndexCoreStorage<Integer, String> coreStorage;
    private WalRuntime<Integer, String> walRuntime;

    @BeforeEach
    void setUp() {
        executorRegistry = new IndexExecutorRegistry(buildConf());
        coreStorage = new SegmentIndexCoreStorageFactory<>(newRequest(),
                new SegmentIndexRuntimeGraphBuilder.ResourceCreationObserver<>() {
                }).create();
        walRuntime = WalRuntime.open(new MemDirectory(), buildConf().getWal(),
                tdi, tds);
    }

    @AfterEach
    void tearDown() {
        RuntimeException failure = null;
        if (walRuntime != null) {
            failure = closeIgnoringFailure(walRuntime::close, failure);
        }
        if (coreStorage != null) {
            failure = closeIgnoringFailure(coreStorage.segmentRegistry()::close,
                    failure);
            failure = closeIgnoringFailure(coreStorage.keyToSegmentMap()::close,
                    failure);
        }
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            failure = closeIgnoringFailure(executorRegistry::close, failure);
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Test
    void createBuildsRuntimeCollaborators() {
        final SegmentIndexRuntimeInputs<Integer, String> request =
                newRequest();
        final SegmentIndexRuntimeSplits<Integer, String> splitState =
                new SegmentIndexSplitInfrastructureFactory<>(request,
                        coreStorage).create();
        final SegmentIndexRuntimeServicesFactory<Integer, String> factory =
                new SegmentIndexRuntimeServicesFactory<>(request, coreStorage,
                        splitState);

        final SegmentIndexRuntimeServices<Integer, String> services =
                factory.create(walRuntime);

        assertNotNull(services.walCoordinator());
        assertNotNull(services.operationAccess());
        assertNotNull(services.maintenanceAccess());
        assertNotNull(services.metricsSnapshotSupplier());
        assertNotNull(services.runtimeLimitApplier());
        assertNotNull(services.controlPlane());
    }

    private SegmentIndexRuntimeInputs<Integer, String> newRequest() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        return new SegmentIndexRuntimeInputs<>(
                LoggerFactory.getLogger(getClass()), new MemDirectory(), tdi,
                tds, conf, conf.resolveRuntimeConfiguration(),
                executorRegistry, new Stats(), new AtomicLong(),
                new AtomicLong(), new AtomicLong(),
                () -> SegmentIndexState.READY, failure -> {
                });
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(tdi)
                .withValueTypeDescriptor(tds)
                .withName("segment-index-runtime-services-factory-test")
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

    private RuntimeException closeIgnoringFailure(final Runnable action,
            final RuntimeException failure) {
        try {
            action.run();
            return failure;
        } catch (final RuntimeException e) {
            if (failure == null) {
                return e;
            }
            failure.addSuppressed(e);
            return failure;
        }
    }
}
