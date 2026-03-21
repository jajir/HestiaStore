package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.partition.PartitionRuntime;
import org.hestiastore.index.segmentindex.split.PartitionStableSplitCoordinator;
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
class SegmentIndexMetricsCollectorTest {

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private Segment<Integer, String> readySegment;

    @Mock
    private Segment<Integer, String> freezeSegment;

    @Mock
    private Segment<Integer, String> maintenanceSegment;

    private KeyToSegmentMapSynchronizedAdapter<Integer> keyToSegmentMap;
    private IndexExecutorRegistry executorRegistry;
    private WalRuntime<Integer, String> walRuntime;
    private CountDownLatch releaseExecutorTasks;

    @BeforeEach
    void setUp() {
        releaseExecutorTasks = new CountDownLatch(1);
    }

    @AfterEach
    void tearDown() {
        if (releaseExecutorTasks != null) {
            releaseExecutorTasks.countDown();
        }
        if (walRuntime != null) {
            walRuntime.close();
        }
        if (executorRegistry != null) {
            executorRegistry.close();
        }
        if (keyToSegmentMap != null && !keyToSegmentMap.wasClosed()) {
            keyToSegmentMap.close();
        }
    }

    @Test
    void metricsSnapshotUsesSplitQueueAndSegmentStateSources() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();
        final KeyToSegmentMap<Integer> keyToSegmentMapDelegate = new KeyToSegmentMap<>(
                new MemDirectory(), keyDescriptor);
        keyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMapDelegate);
        keyToSegmentMap.insertSegment(Integer.valueOf(10), SegmentId.of(1));
        keyToSegmentMap.insertSegment(Integer.valueOf(20), SegmentId.of(2));
        keyToSegmentMap.insertSegment(Integer.valueOf(30), SegmentId.of(3));

        executorRegistry = new IndexExecutorRegistry(conf);
        walRuntime = WalRuntime.open(new MemDirectory(), conf.getWal(),
                keyDescriptor, valueDescriptor);
        final PartitionRuntime<Integer, String> partitionRuntime = new PartitionRuntime<>(
                keyDescriptor.getComparator());
        final PartitionStableSplitCoordinator<Integer, String> splitCoordinator = new PartitionStableSplitCoordinator<>(
                conf, keyDescriptor.getComparator(), keyToSegmentMap,
                segmentRegistry, partitionRuntime);
        final BackgroundSplitCoordinator<Integer, String> backgroundSplitCoordinator = new BackgroundSplitCoordinator<>(
                keyToSegmentMap, partitionRuntime, splitCoordinator,
                executorRegistry.getSplitMaintenanceExecutor(), e -> {
                }, () -> {
                });
        setSplitInFlightCount(backgroundSplitCoordinator, 2);

        when(segmentRegistry.metricsSnapshot())
                .thenReturn(SegmentRegistryCacheStats.empty());
        when(segmentRegistry.loadedSegmentsSnapshot()).thenReturn(List.of(
                readySegment, freezeSegment, maintenanceSegment));
        when(readySegment.getRuntimeSnapshot()).thenReturn(
                runtimeSnapshot(1, SegmentState.READY, 11L));
        when(freezeSegment.getRuntimeSnapshot()).thenReturn(
                runtimeSnapshot(2, SegmentState.FREEZE, 7L));
        when(maintenanceSegment.getRuntimeSnapshot()).thenReturn(
                runtimeSnapshot(3, SegmentState.MAINTENANCE_RUNNING, 5L));

        final Stats stats = new Stats();
        stats.incSplitScheduleCx();
        stats.incSplitScheduleCx();
        stats.incSplitScheduleCx();

        queueOneTask(executorRegistry.getIndexMaintenanceExecutor(),
                releaseExecutorTasks);
        queueOneTask(executorRegistry.getSplitMaintenanceExecutor(),
                releaseExecutorTasks);
        awaitCondition(() -> executorRegistry.indexMaintenanceQueueSize() == 1
                && executorRegistry.splitMaintenanceQueueSize() == 1,
                2_000L);

        final SegmentIndexMetricsCollector<Integer, String> collector = new SegmentIndexMetricsCollector<>(
                conf, keyToSegmentMap, segmentRegistry, partitionRuntime,
                backgroundSplitCoordinator, executorRegistry,
                RuntimeTuningState.fromConfiguration(conf), walRuntime, stats,
                new AtomicLong(), new AtomicLong(), new AtomicLong(),
                () -> SegmentIndexState.READY);

        final SegmentIndexMetricsSnapshot snapshot = collector
                .metricsSnapshot();

        assertEquals(3, snapshot.getSegmentCount());
        assertEquals(1, snapshot.getSegmentReadyCount());
        assertEquals(1, snapshot.getSegmentMaintenanceCount());
        assertEquals(1, snapshot.getSegmentBusyCount());
        assertEquals(3L, snapshot.getSplitScheduleCount());
        assertEquals(2, snapshot.getSplitInFlightCount());
        assertEquals(1, snapshot.getMaintenanceQueueSize());
        assertEquals(64, snapshot.getMaintenanceQueueCapacity());
        assertEquals(1, snapshot.getSplitQueueSize());
        assertEquals(64, snapshot.getSplitQueueCapacity());
        assertEquals(0L, snapshot.getDrainScheduleCount());
        assertEquals(0, snapshot.getDrainInFlightCount());
        assertEquals(SegmentIndexState.READY, snapshot.getState());
    }

    private static IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withName("segment-index-metrics-collector-test")//
                .withContextLoggingEnabled(false)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInActivePartition(5)//
                .withMaxNumberOfImmutableRunsPerPartition(2)//
                .withMaxNumberOfKeysInPartitionBuffer(6)//
                .withMaxNumberOfKeysInIndexBuffer(12)//
                .withMaxNumberOfKeysInPartitionBeforeSplit(8)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInSegment(100)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withIndexWorkerThreadCount(1)//
                .withNumberOfIndexMaintenanceThreads(1)//
                .withNumberOfStableSegmentMaintenanceThreads(1)//
                .withNumberOfRegistryLifecycleThreads(1)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }

    private static SegmentRuntimeSnapshot runtimeSnapshot(final int segmentId,
            final SegmentState state, final long numberOfKeysInSegment) {
        return new SegmentRuntimeSnapshot(SegmentId.of(segmentId), state, 0L,
                numberOfKeysInSegment, 0L, 0L, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L);
    }

    private static void queueOneTask(final ExecutorService executor,
            final CountDownLatch releaseLatch) {
        final CountDownLatch running = new CountDownLatch(1);
        executor.submit(() -> awaitRelease(running, releaseLatch));
        assertTrue(await(running, 2_000L),
                "Worker task did not start in time.");
        executor.submit(() -> awaitRelease(new CountDownLatch(0), releaseLatch));
    }

    private static void awaitRelease(final CountDownLatch running,
            final CountDownLatch releaseLatch) {
        running.countDown();
        try {
            releaseLatch.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while holding executor slot.", e);
        }
    }

    private static boolean await(final CountDownLatch latch,
            final long timeoutMillis) {
        try {
            return latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while waiting for latch.", e);
        }
    }

    private static void awaitCondition(final java.util.function.BooleanSupplier condition,
            final long timeoutMillis) {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
            if (Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Interrupted while waiting for condition.");
            }
        }
        assertTrue(condition.getAsBoolean(),
                "Condition not reached within " + timeoutMillis + " ms.");
    }

    private static void setSplitInFlightCount(
            final BackgroundSplitCoordinator<?, ?> coordinator,
            final int splitInFlightCount) {
        try {
            final Field field = BackgroundSplitCoordinator.class
                    .getDeclaredField("splitInFlightCount");
            field.setAccessible(true);
            field.setInt(coordinator, splitInFlightCount);
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Unable to set split-in-flight count for test.", e);
        }
    }
}
