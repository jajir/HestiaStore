package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.infrastructure.IndexExecutorRegistry;
import org.hestiastore.index.segmentindex.core.state.IndexStateCoordinator;
import org.hestiastore.index.segmentindex.core.state.IndexStateOpening;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SegmentIndexCoreInputsTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final TypeDescriptorInteger keyTypeDescriptor =
            new TypeDescriptorInteger();
    private final TypeDescriptorShortString valueTypeDescriptor =
            new TypeDescriptorShortString();

    private IndexExecutorRegistry executorRegistry;

    @AfterEach
    void tearDown() {
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            executorRegistry.close();
        }
    }

    @Test
    void createStoresValidatedAssemblyInputs() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = buildConf();
        final IndexRuntimeConfiguration<Integer, String> runtimeConfiguration =
                conf.resolveRuntimeConfiguration();
        executorRegistry = new IndexExecutorRegistry(conf);
        final IndexStateCoordinator<Integer, String> stateCoordinator =
                new IndexStateCoordinator<>(new IndexStateOpening<>(directory),
                        SegmentIndexState.OPENING);

        final SegmentIndexCoreInputs<Integer, String> request =
                SegmentIndexCoreInputs.create(logger, directory,
                        keyTypeDescriptor, valueTypeDescriptor, conf,
                        runtimeConfiguration, executorRegistry, stateCoordinator,
                        true);

        assertSame(logger, request.logger);
        assertSame(directory, request.directoryFacade);
        assertSame(keyTypeDescriptor, request.keyTypeDescriptor);
        assertSame(valueTypeDescriptor, request.valueTypeDescriptor);
        assertSame(conf, request.conf);
        assertSame(runtimeConfiguration, request.runtimeConfiguration);
        assertSame(executorRegistry, request.executorRegistry);
        assertSame(stateCoordinator, request.stateCoordinator);
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(keyTypeDescriptor)
                .withValueTypeDescriptor(valueTypeDescriptor)
                .withName("segment-index-core-assembly-request-test")
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
