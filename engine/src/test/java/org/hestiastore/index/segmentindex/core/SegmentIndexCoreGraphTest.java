package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.infrastructure.IndexExecutorRegistry;
import org.hestiastore.index.segmentindex.core.runtime.SegmentIndexRuntimeTestAccess;
import org.hestiastore.index.segmentindex.core.state.IndexStateCoordinator;
import org.hestiastore.index.segmentindex.core.state.IndexStateOpening;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class SegmentIndexCoreGraphTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    private IndexExecutorRegistry executorRegistry;
    private SegmentIndexCoreGraph<Integer, String> composition;

    @AfterEach
    void tearDown() {
        RuntimeException cleanupFailure = null;
        if (composition != null) {
            cleanupFailure = closeIgnoringFailure(
                    () -> SegmentIndexRuntimeTestAccess
                            .walRuntime(composition.runtime()).close(),
                    cleanupFailure);
            cleanupFailure = closeIgnoringFailure(
                    () -> SegmentIndexRuntimeTestAccess
                            .segmentRegistry(composition.runtime()).close(),
                    cleanupFailure);
            cleanupFailure = closeIgnoringFailure(
                    () -> SegmentIndexRuntimeTestAccess
                            .keyToSegmentMap(composition.runtime()).close(),
                    cleanupFailure);
        }
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            cleanupFailure = closeIgnoringFailure(executorRegistry::close,
                    cleanupFailure);
        }
        if (cleanupFailure != null) {
            throw cleanupFailure;
        }
    }

    @Test
    void assembleBuildsRuntimeFacadesAndCoordinators() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = buildConf();
        executorRegistry = new IndexExecutorRegistry(conf);
        final IndexStateCoordinator<Integer, String> stateCoordinator =
                new IndexStateCoordinator<>(new IndexStateOpening<>(directory),
                        SegmentIndexState.OPENING);

        composition = SegmentIndexCoreGraph.create(
                SegmentIndexCoreInputs.create(
                        LoggerFactory.getLogger(getClass()), directory, tdi,
                        tds, conf, conf.resolveRuntimeConfiguration(),
                        executorRegistry, stateCoordinator, false));

        assertNotNull(composition.runtime());
        assertNotNull(composition.mutationFacade());
        assertNotNull(composition.readFacade());
        assertNotNull(composition.maintenanceCommands());
        assertNotNull(composition.maintenanceAccess());
        assertNotNull(composition.closeCoordinator());
        assertNotNull(composition.startupCoordinator());
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(tdi)
                .withValueTypeDescriptor(tds)
                .withName("segment-index-core-composition-test")
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
