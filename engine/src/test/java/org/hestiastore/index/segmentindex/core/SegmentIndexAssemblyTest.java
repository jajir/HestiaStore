package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SegmentIndexAssemblyTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    private IndexExecutorRegistry executorRegistry;
    private SegmentIndexAssembly<Integer, String> assembly;

    @AfterEach
    void tearDown() {
        RuntimeException cleanupFailure = null;
        if (assembly != null) {
            cleanupFailure = closeIgnoringAdditionalFailure(
                    assembly.closeCoordinator()::close, cleanupFailure);
            cleanupFailure = closeIgnoringAdditionalFailure(
                    assembly.runtime().keyToSegmentMap()::close,
                    cleanupFailure);
        }
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            cleanupFailure = closeIgnoringAdditionalFailure(
                    executorRegistry::close, cleanupFailure);
        }
        if (cleanupFailure != null) {
            fail(cleanupFailure);
        }
    }

    @BeforeEach
    void setUp() {
        executorRegistry = new IndexExecutorRegistry(buildConf());
    }

    @Test
    void openBuildsRuntimeAndMarksIndexReady() {
        final AtomicInteger markReadyCalls = new AtomicInteger();
        assembly = openAssembly(() -> {
        }, () -> {
        }, () -> {
        });
        assembly.completeOpen(logger, buildConf().getIndexName(), false,
                () -> markReadyCalls.incrementAndGet(), () -> {
                });

        assertNotNull(assembly.runtime());
        assertNotNull(assembly.consistencyCoordinator());
        assertNotNull(assembly.closeCoordinator());
        assertEquals(1, markReadyCalls.get());
    }

    @Test
    void closeCoordinatorUsesAssemblyCallbacks() {
        final AtomicInteger beginCloseCalls = new AtomicInteger();
        final AtomicInteger markClosedCalls = new AtomicInteger();
        final AtomicInteger finishCloseCalls = new AtomicInteger();
        assembly = openAssembly(() -> beginCloseCalls.incrementAndGet(),
                () -> markClosedCalls.incrementAndGet(),
                () -> finishCloseCalls.incrementAndGet());

        assembly.closeCoordinator().close();
        assembly.runtime().keyToSegmentMap().close();
        assembly = null;

        assertEquals(1, beginCloseCalls.get());
        assertEquals(1, markClosedCalls.get());
        assertEquals(1, finishCloseCalls.get());
    }

    @Test
    void completeOpenRunsConsistencyCheckWhenStaleLockRecovered() {
        final AtomicInteger consistencyChecks = new AtomicInteger();
        assembly = openAssembly(() -> {
        }, () -> {
        }, () -> {
        });

        assembly.completeOpen(logger, buildConf().getIndexName(), true,
                () -> {
                }, () -> consistencyChecks.incrementAndGet());

        assertEquals(1, consistencyChecks.get());
    }

    private SegmentIndexAssembly<Integer, String> openAssembly(
            final Runnable beginCloseTransition,
            final Runnable markClosed, final Runnable finishCloseTransition) {
        final IndexConfiguration<Integer, String> conf = buildConf();
        final AtomicReference<RuntimeException> failureRef = new AtomicReference<>();
        return SegmentIndexAssembly.open(logger, new MemDirectory(), tdi, tds,
                conf, conf.resolveRuntimeConfiguration(), executorRegistry,
                new Stats(), new AtomicLong(),
                new AtomicLong(), new AtomicLong(),
                new SegmentIndexAssembly.Callbacks(
                        () -> SegmentIndexState.READY, () -> {
                        }, failureRef::set, () -> {
                        }, beginCloseTransition, () -> {
                        }, markClosed, () -> 0L, () -> 0L,
                        () -> 0L, finishCloseTransition));
    }

    private SegmentIndexAssembly<Integer, String> openAssembly(
            final Runnable beginCloseTransition,
            final Runnable markClosed) {
        return openAssembly(beginCloseTransition, markClosed, () -> {
        });
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(tdi)
                .withValueTypeDescriptor(tds)
                .withName("segment-index-assembly-test")
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
                .withIndexWorkerThreadCount(1)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }

    private RuntimeException closeIgnoringAdditionalFailure(
            final Runnable action, final RuntimeException failure) {
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
