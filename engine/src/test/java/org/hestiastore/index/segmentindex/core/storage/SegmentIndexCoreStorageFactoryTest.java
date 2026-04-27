package org.hestiastore.index.segmentindex.core.storage;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexRuntimeGraphBuilder;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexRuntimeInputs;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class SegmentIndexCoreStorageFactoryTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    private ExecutorRegistry executorRegistry;
    private SegmentIndexCoreStorage<Integer, String> coreStorage;

    @BeforeEach
    void setUp() {
        executorRegistry = ExecutorRegistryFixture.from(buildConf());
    }

    @AfterEach
    void tearDown() {
        RuntimeException failure = null;
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
    void createBuildsCoreStorageAndNotifiesObserver() {
        final AtomicReference<KeyToSegmentMap<Integer>> keyToSegmentMapRef =
                new AtomicReference<>();
        final AtomicReference<SegmentRegistry<Integer, String>> segmentRegistryRef =
                new AtomicReference<>();
        final SegmentIndexRuntimeInputs<Integer, String> request =
                newRequest();
        final SegmentIndexCoreStorageFactory<Integer, String> factory =
                new SegmentIndexCoreStorageFactory<>(request,
                        new SegmentIndexRuntimeGraphBuilder.ResourceCreationObserver<>() {
                            @Override
                            public void onKeyToSegmentMapCreated(
                                    final KeyToSegmentMap<Integer> keyToSegmentMap) {
                                keyToSegmentMapRef.set(keyToSegmentMap);
                            }

                            @Override
                            public void onSegmentRegistryCreated(
                                    final SegmentRegistry<Integer, String> segmentRegistry) {
                                segmentRegistryRef.set(segmentRegistry);
                            }
                        });

        coreStorage = factory.create();

        assertNotNull(coreStorage.runtimeTuningState());
        assertSame(coreStorage.keyToSegmentMap(), keyToSegmentMapRef.get());
        assertSame(coreStorage.segmentRegistry(), segmentRegistryRef.get());
        assertNotNull(coreStorage.retryPolicy());
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
                .withName("segment-index-core-storage-factory-test")
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
