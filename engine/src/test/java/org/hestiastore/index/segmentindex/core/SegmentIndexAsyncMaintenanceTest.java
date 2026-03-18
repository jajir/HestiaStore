package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.SegmentRegistryCache;
import org.hestiastore.index.segmentregistry.SegmentRegistryImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexAsyncMaintenanceTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    private IndexInternalConcurrent<Integer, String> index;

    @Mock
    private Segment<Integer, String> blockingSegment;

    @BeforeEach
    void setUp() {
        index = newIndex();
    }

    @AfterEach
    void tearDown() {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
    }

    @Test
    void flush_waits_for_async_segment_flush() throws Exception {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            index.put(1, "one");
            index.flushAndWait();

            final SegmentId segmentId = readKeyToSegmentMap(index)
                    .findSegmentId(1);
            final SegmentRegistryImpl<Integer, String> registry = readSegmentRegistry(
                    index);
            final Segment<Integer, String> originalSegment = registry
                    .getSegment(segmentId).getValue();
            final CountDownLatch started = new CountDownLatch(1);
            final AtomicReference<SegmentState> stateRef = new AtomicReference<>(
                    SegmentState.READY);
            final Segment<Integer, String> mockedSegment = mockBlockingSegment(
                    started, stateRef, true);
            replaceSegment(registry, segmentId, mockedSegment);

            final Future<?> flushTask = executor.submit(index::flushAndWait);
            assertTrue(started.await(1, TimeUnit.SECONDS));
            assertFalse(flushTask.isDone());
            assertNotNull(stateRef.get());
            stateRef.set(SegmentState.READY);
            flushTask.get(1, TimeUnit.SECONDS);
            replaceSegment(registry, segmentId, originalSegment);
        } finally {
            executor.shutdownNow();
            index.close();
        }
    }

    @Test
    void compact_waits_for_async_segment_compact() throws Exception {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            index.put(1, "one");
            index.flushAndWait();

            final SegmentId segmentId = readKeyToSegmentMap(index)
                    .findSegmentId(1);
            final SegmentRegistryImpl<Integer, String> registry = readSegmentRegistry(
                    index);
            final Segment<Integer, String> originalSegment = registry
                    .getSegment(segmentId).getValue();
            final CountDownLatch started = new CountDownLatch(1);
            final AtomicReference<SegmentState> stateRef = new AtomicReference<>(
                    SegmentState.READY);
            final Segment<Integer, String> mockedSegment = mockBlockingSegment(
                    started, stateRef, false);
            replaceSegment(registry, segmentId, mockedSegment);

            final Future<?> compactTask = executor.submit(index::compactAndWait);
            assertTrue(started.await(1, TimeUnit.SECONDS));
            assertFalse(compactTask.isDone());
            assertNotNull(stateRef.get());
            stateRef.set(SegmentState.READY);
            compactTask.get(1, TimeUnit.SECONDS);
            replaceSegment(registry, segmentId, originalSegment);
        } finally {
            executor.shutdownNow();
            index.close();
        }
    }

    @Test
    void closeStopsAutonomousSplitPolicyLoop() throws Exception {
        index.put(1, "one");
        index.flushAndWait();

        index.close();
        TimeUnit.MILLISECONDS.sleep(750L);

        assertEquals(SegmentIndexState.CLOSED, index.getState());
    }

    @Test
    void closeExposesClosingStateWhileBlockedPartitionDrainPublishes() throws Exception {
        index.close();
        index = newDrainIndex();

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final BlockedDrainHarness harness = installBlockedDrainHarness(index);
            try {
                final Future<?> closeTask = executor.submit(index::close);

                awaitCondition(() -> index.getState() == SegmentIndexState.CLOSING,
                        5_000L);
                assertFalse(closeTask.isDone());
                assertEquals(SegmentIndexState.CLOSING, index.metricsSnapshot()
                        .getState());

                harness.release().countDown();
                closeTask.get(5, TimeUnit.SECONDS);
            } finally {
                harness.release().countDown();
            }

            assertEquals(SegmentIndexState.CLOSED, index.getState());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void flush_waits_for_blocked_partition_drain_publish() throws Exception {
        index.close();
        index = newDrainIndex();

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final BlockedDrainHarness harness = installBlockedDrainHarness(index);
            try {
                final Future<?> flushTask = executor.submit(index::flushAndWait);
                assertFalse(flushTask.isDone());

                final SegmentIndexMetricsSnapshot blockedSnapshot = index
                        .metricsSnapshot();
                assertTrue(blockedSnapshot.getDrainInFlightCount() > 0);
                assertTrue(blockedSnapshot.getDrainingPartitionCount() > 0);
                assertTrue(blockedSnapshot.getImmutableRunCount() > 0);

                harness.release().countDown();
                flushTask.get(5, TimeUnit.SECONDS);
            } finally {
                harness.release().countDown();
                replaceSegment(harness.registry(), harness.segmentId(),
                        harness.originalSegment());
            }

            awaitCondition(() -> {
                final SegmentIndexMetricsSnapshot snapshot = index
                        .metricsSnapshot();
                return snapshot.getDrainInFlightCount() == 0
                        && snapshot.getDrainingPartitionCount() == 0
                        && snapshot.getImmutableRunCount() == 0;
            }, 5_000L);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void compact_waits_for_blocked_partition_drain_publish() throws Exception {
        index.close();
        index = newDrainIndex();

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final BlockedDrainHarness harness = installBlockedDrainHarness(index);
            try {
                final Future<?> compactTask = executor
                        .submit(index::compactAndWait);
                assertFalse(compactTask.isDone());

                final SegmentIndexMetricsSnapshot blockedSnapshot = index
                        .metricsSnapshot();
                assertTrue(blockedSnapshot.getDrainInFlightCount() > 0);
                assertTrue(blockedSnapshot.getDrainingPartitionCount() > 0);
                assertTrue(blockedSnapshot.getImmutableRunCount() > 0);

                harness.release().countDown();
                compactTask.get(5, TimeUnit.SECONDS);
            } finally {
                harness.release().countDown();
                replaceSegment(harness.registry(), harness.segmentId(),
                        harness.originalSegment());
            }

            awaitCondition(() -> {
                final SegmentIndexMetricsSnapshot snapshot = index
                        .metricsSnapshot();
                return snapshot.getDrainInFlightCount() == 0
                        && snapshot.getDrainingPartitionCount() == 0
                        && snapshot.getImmutableRunCount() == 0;
            }, 5_000L);
        } finally {
            executor.shutdownNow();
        }
    }

    private IndexInternalConcurrent<Integer, String> newIndex() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        return new IndexInternalConcurrent<>(
                new MemDirectory(),
                tdi, tds, conf, new IndexExecutorRegistry(conf));
    }

    private IndexInternalConcurrent<Integer, String> newDrainIndex() {
        final IndexConfiguration<Integer, String> conf = buildDrainConf();
        return new IndexInternalConcurrent<>(
                new MemDirectory(),
                tdi, tds, conf, new IndexExecutorRegistry(conf));
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withName("async-maintenance-test")//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInActivePartition(5)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInSegment(100)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withContextLoggingEnabled(false)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withIndexWorkerThreadCount(1)//
                .build();
    }

    private IndexConfiguration<Integer, String> buildDrainConf() {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withName("async-maintenance-drain-test")//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInActivePartition(4)//
                .withMaxNumberOfImmutableRunsPerPartition(1)//
                .withMaxNumberOfKeysInPartitionBuffer(16)//
                .withMaxNumberOfKeysInIndexBuffer(32)//
                .withMaxNumberOfKeysInPartitionBeforeSplit(10_000_000)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInSegment(100)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withContextLoggingEnabled(false)//
                .withBackgroundMaintenanceAutoEnabled(false)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withIndexWorkerThreadCount(1)//
                .build();
    }

    private Segment<Integer, String> mockBlockingSegment(
            final CountDownLatch started,
            final AtomicReference<SegmentState> stateRef,
            final boolean forFlush) {
        when(blockingSegment.getState()).thenAnswer(
                invocation -> stateRef.get());
        lenient().when(blockingSegment.flush()).thenAnswer(invocation -> {
            if (forFlush) {
                stateRef.set(SegmentState.MAINTENANCE_RUNNING);
                started.countDown();
            }
            return SegmentResult.ok();
        });
        lenient().when(blockingSegment.compact()).thenAnswer(invocation -> {
            if (!forFlush) {
                stateRef.set(SegmentState.MAINTENANCE_RUNNING);
                started.countDown();
            }
            return SegmentResult.ok();
        });
        return blockingSegment;
    }

    private BlockedDrainHarness installBlockedDrainHarness(
            final IndexInternalConcurrent<Integer, String> drainingIndex)
            throws Exception {
        if (readKeyToSegmentMap(drainingIndex).getSegmentIds().isEmpty()) {
            drainingIndex.put(0, "seed-0");
        }
        final SegmentId segmentId = readKeyToSegmentMap(drainingIndex)
                .getSegmentIds().get(0);
        final SegmentRegistryImpl<Integer, String> registry = readSegmentRegistry(
                drainingIndex);
        final Segment<Integer, String> originalSegment = registry
                .getSegment(segmentId).getValue();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Segment<Integer, String> blockedSegment = mockBlockedDrainSegment(
                segmentId, started, release);
        replaceSegment(registry, segmentId, blockedSegment);

        for (int i = 1; i < 32; i++) {
            drainingIndex.put(i, "value-" + i);
            if (started.await(20, TimeUnit.MILLISECONDS)) {
                awaitCondition(() -> {
                    final SegmentIndexMetricsSnapshot snapshot = drainingIndex
                            .metricsSnapshot();
                    return snapshot.getDrainInFlightCount() > 0
                            && snapshot.getDrainingPartitionCount() > 0;
                }, 5_000L);
                return new BlockedDrainHarness(segmentId, registry,
                        originalSegment, release);
            }
        }
        replaceSegment(registry, segmentId, originalSegment);
        throw new AssertionError("Blocked drain did not start in time.");
    }

    @SuppressWarnings("unchecked")
    private Segment<Integer, String> mockBlockedDrainSegment(
            final SegmentId segmentId, final CountDownLatch started,
            final CountDownLatch release) {
        final AtomicReference<SegmentState> stateRef = new AtomicReference<>(
                SegmentState.READY);
        final Segment<Integer, String> blockedSegment = mock(Segment.class);
        lenient().when(blockedSegment.getId()).thenReturn(segmentId);
        lenient().when(blockedSegment.getState()).thenAnswer(
                invocation -> stateRef.get());
        lenient().when(blockedSegment.getRuntimeSnapshot()).thenAnswer(
                invocation -> new SegmentRuntimeSnapshot(segmentId,
                        stateRef.get(), 0L, 0L, 0L, 0L, 0, 0, 0L, 0L, 0L, 0L,
                        0L, 0L));
        when(blockedSegment.put(any(), any())).thenAnswer(invocation -> {
            started.countDown();
            if (!release.await(5, TimeUnit.SECONDS)) {
                throw new TimeoutException(
                        "Timed out waiting to release blocked drain.");
            }
            return SegmentResult.ok();
        });
        lenient().when(blockedSegment.flush()).thenReturn(SegmentResult.ok());
        lenient().when(blockedSegment.compact()).thenReturn(SegmentResult.ok());
        lenient().when(blockedSegment.close()).thenAnswer(invocation -> {
            stateRef.set(SegmentState.CLOSED);
            return SegmentResult.ok();
        });
        return blockedSegment;
    }

    @SuppressWarnings("unused")
    private void triggerSplitScan(final IndexInternalConcurrent<Integer, String> drainingIndex) {
        final long revision = drainingIndex.controlPlane().configuration()
                .getConfigurationActual().getRevision();
        final RuntimePatchResult patchResult = drainingIndex.controlPlane()
                .configuration()
                .apply(new RuntimeConfigPatch(Map.of(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
                        Integer.valueOf(16)), false,
                        Long.valueOf(revision)));
        assertTrue(patchResult.isApplied());
    }

    private static void awaitCondition(final Supplier<Boolean> condition,
            final long timeoutMillis) {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.get()) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20L));
            if (Thread.currentThread().isInterrupted()) {
                throw new AssertionError("Interrupted while waiting");
            }
        }
        assertTrue(condition.get(),
                "Condition not reached within " + timeoutMillis + " ms.");
    }

    private record BlockedDrainHarness(SegmentId segmentId,
            SegmentRegistryImpl<Integer, String> registry,
            Segment<Integer, String> originalSegment,
            CountDownLatch release) {
    }

    private static <K, V> void replaceSegment(
            final SegmentRegistryImpl<K, V> registry, final SegmentId segmentId,
            final Segment<K, V> segment) {
        final SegmentRegistryCache<K, V> cache = readCache(registry);
        putCacheEntry(cache, segmentId, segment);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentRegistryImpl<K, V> readSegmentRegistry(
            final SegmentIndexImpl<K, V> index) {
        return (SegmentRegistryImpl<K, V>) index.segmentRegistry();
    }

    @SuppressWarnings("unchecked")
    private static <K, V> KeyToSegmentMapSynchronizedAdapter<K> readKeyToSegmentMap(
            final SegmentIndexImpl<K, V> index) {
        return index.keyToSegmentMap();
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentRegistryCache<K, V> readCache(
            final SegmentRegistryImpl<K, V> registry) {
        try {
            final Field cacheField = SegmentRegistryImpl.class
                    .getDeclaredField("cache");
            cacheField.setAccessible(true);
            return (SegmentRegistryCache<K, V>) cacheField.get(registry);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read registry cache for test", ex);
        }
    }

    private static void putCacheEntry(final Object cache,
            final SegmentId segmentId, final Segment<?, ?> segment) {
        try {
            final Field mapField = cache.getClass().getDeclaredField("map");
            mapField.setAccessible(true);
            @SuppressWarnings("unchecked")
            final java.util.concurrent.ConcurrentHashMap<SegmentId, Object> map = (java.util.concurrent.ConcurrentHashMap<SegmentId, Object>) mapField
                    .get(cache);
            final Class<?> entryClass = Class.forName(
                    SegmentRegistryCache.class.getName() + "$Entry");
            final var ctor = entryClass.getDeclaredConstructor(long.class);
            ctor.setAccessible(true);
            final Object entry = ctor.newInstance(0L);
            final Field stateField = entryClass.getDeclaredField("state");
            stateField.setAccessible(true);
            final Class<?> stateClass = Class.forName(
                    SegmentRegistryCache.class.getName() + "$EntryState");
            stateField.set(entry, Enum.valueOf(
                    stateClass.asSubclass(Enum.class), "READY"));
            final Field valueField = entryClass.getDeclaredField("value");
            valueField.setAccessible(true);
            valueField.set(entry, segment);
            map.put(segmentId, entry);
            final Field sizeField = cache.getClass().getDeclaredField("size");
            sizeField.setAccessible(true);
            final java.util.concurrent.atomic.AtomicInteger size = (java.util.concurrent.atomic.AtomicInteger) sizeField
                    .get(cache);
            size.incrementAndGet();
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to update registry cache for test", ex);
        }
    }
}
