package org.hestiastore.index.segmentindex;

import static org.hestiastore.index.segment.SegmentTestHelper.closeAndAwait;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryImpl;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;
import org.hestiastore.index.segmentregistry.SegmentRegistryStateMachine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentRegistryImplTest {

    private static final TypeDescriptorInteger KEY_DESCRIPTOR = new TypeDescriptorInteger();
    private static final TypeDescriptorShortString VALUE_DESCRIPTOR = new TypeDescriptorShortString();
    private static final List<ChunkFilter> FILTERS = List
            .of(new ChunkFilterDoNothing());

    private AsyncDirectory directoryFacade;

    @Mock
    private IndexConfiguration<Integer, String> conf;

    private SegmentRegistryImpl<Integer, String> registry;
    private SegmentAsyncExecutor maintenanceExecutor;

    @BeforeEach
    void setUp() {
        Mockito.when(conf.getMaxNumberOfSegmentsInCache()).thenReturn(3);
        directoryFacade = AsyncDirectoryAdapter.wrap(new MemDirectory());
        maintenanceExecutor = new SegmentAsyncExecutor(1,
                "segment-maintenance");
        registry = (SegmentRegistryImpl<Integer, String>) SegmentRegistry
                .<Integer, String>builder().withDirectoryFacade(directoryFacade)
                .withKeyTypeDescriptor(KEY_DESCRIPTOR)
                .withValueTypeDescriptor(VALUE_DESCRIPTOR)
                .withConfiguration(conf)
                .withMaintenanceExecutor(maintenanceExecutor.getExecutor())
                .withLifecycleExecutor(Executors.newSingleThreadExecutor())
                .build();
    }

    @AfterEach
    void tearDown() {
        if (registry != null) {
            registry.close();
        }
        if (maintenanceExecutor != null && !maintenanceExecutor.wasClosed()) {
            maintenanceExecutor.close();
        }
    }

    @Test
    void getSegment_reusesInstanceUntilClosed() {
        stubSegmentConfig();
        final Segment<Integer, String> created = registry.createSegment()
                .getValue();
        final SegmentId segmentId = created.getId();

        final Segment<Integer, String> first = registry.getSegment(segmentId)
                .getValue();
        final Segment<Integer, String> second = registry.getSegment(segmentId)
                .getValue();

        assertSame(first, second);
        closeAndAwait(first);
        final Segment<Integer, String> third = registry.getSegment(segmentId)
                .getValue();

        assertNotSame(first, third);
    }

    @Test
    void registryStartupTransitionsGateToReady() {
        final SegmentRegistryStateMachine gate = readGate(registry);
        assertSame(
                org.hestiastore.index.segmentregistry.SegmentRegistryState.READY,
                gate.getState());
    }

    @Test
    void getSegment_returnsBusyWhileRegistryFrozen() {
        final SegmentId segmentId = SegmentId.of(1);

        final SegmentRegistryStateMachine gate = readGate(registry);
        assertTrue(gate.tryEnterFreeze());
        try (GateGuard ignored = new GateGuard(gate)) {
            final SegmentRegistryResult<Segment<Integer, String>> result = registry
                    .getSegment(segmentId);
            assertSame(SegmentRegistryResultStatus.BUSY, result.getStatus());
        }
    }

    @Test
    void getSegment_returnsClosedWhenRegistryClosed() {
        registry.close();

        final SegmentRegistryResult<Segment<Integer, String>> result = registry
                .getSegment(SegmentId.of(1));
        assertSame(SegmentRegistryResultStatus.CLOSED, result.getStatus());
    }

    @Test
    void getSegment_returnsErrorWhenRegistryIsInErrorState() {
        final SegmentRegistryStateMachine gate = readGate(registry);
        gate.fail();

        final SegmentRegistryResult<Segment<Integer, String>> result = registry
                .getSegment(SegmentId.of(1));
        assertSame(SegmentRegistryResultStatus.ERROR, result.getStatus());
    }

    @Test
    void getSegment_evicts_least_recently_used_when_limit_exceeded() {
        Mockito.when(conf.getMaxNumberOfSegmentsInCache()).thenReturn(2);
        registry.close();
        registry = (SegmentRegistryImpl<Integer, String>) SegmentRegistry
                .<Integer, String>builder().withDirectoryFacade(directoryFacade)
                .withKeyTypeDescriptor(KEY_DESCRIPTOR)
                .withValueTypeDescriptor(VALUE_DESCRIPTOR)
                .withConfiguration(conf)
                .withMaintenanceExecutor(maintenanceExecutor.getExecutor())
                .withLifecycleExecutor(Executors.newSingleThreadExecutor())
                .build();
        stubSegmentConfig();
        final Segment<Integer, String> first = registry.createSegment()
                .getValue();
        final Segment<Integer, String> second = registry.createSegment()
                .getValue();
        final Segment<Integer, String> third = registry.createSegment()
                .getValue();

        final long closedCount = List.of(first, second, third).stream()
                .filter(segment -> segment.getState() == SegmentState.CLOSED)
                .count();
        if (closedCount == 0L) {
            awaitCondition(() -> List.of(first, second, third).stream()
                    .anyMatch(segment -> segment
                            .getState() == SegmentState.CLOSED),
                    1500L);
        }
        assertTrue(List.of(first, second, third).stream().anyMatch(
                segment -> segment.getState() == SegmentState.CLOSED));
    }

    @Test
    void getSegment_returnsErrorWhenSegmentMissing() {
        stubSegmentConfig();
        final Segment<Integer, String> existing = registry.createSegment()
                .getValue();
        final SegmentId missingId = SegmentId.of(existing.getId().getId() + 1);

        final SegmentRegistryResult<Segment<Integer, String>> result = registry
                .getSegment(missingId);
        assertSame(SegmentRegistryResultStatus.ERROR, result.getStatus());
    }

    @Test
    void getSegment_waitsForLoadingEntryAndReturnsOkAfterFinishLoad()
            throws Exception {
        final SegmentId segmentId = SegmentId.of(999);
        final Object entry = createLoadingEntry(10L);
        putCacheEntry(segmentId, entry);

        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = Mockito.mock(Segment.class);
        Mockito.when(segment.getState()).thenReturn(SegmentState.READY);

        final ExecutorService callers = Executors.newSingleThreadExecutor();
        try {
            final Future<SegmentRegistryResult<Segment<Integer, String>>> waiter = callers
                    .submit(() -> registry.getSegment(segmentId));

            org.junit.jupiter.api.Assertions.assertThrows(
                    TimeoutException.class,
                    () -> waiter.get(100, TimeUnit.MILLISECONDS));

            finishLoad(entry, segment);

            final SegmentRegistryResult<Segment<Integer, String>> result = waiter
                    .get(1, TimeUnit.SECONDS);
            assertSame(SegmentRegistryResultStatus.OK, result.getStatus());
            assertSame(segment, result.getValue());
        } finally {
            callers.shutdownNow();
            removeCacheEntry(segmentId);
        }
    }

    @Test
    void getSegment_returnsBusyWhenEntryIsUnloading() throws Exception {
        stubSegmentConfig();
        final Segment<Integer, String> created = registry.createSegment()
                .getValue();
        final SegmentId segmentId = created.getId();
        final Segment<Integer, String> segment = registry.getSegment(segmentId)
                .getValue();
        assertNotNull(segment);

        final Object entry = getCacheEntry(segmentId);
        assertTrue(invokeTryStartUnload(entry));

        final SegmentRegistryResult<Segment<Integer, String>> result = registry
                .getSegment(segmentId);
        assertSame(SegmentRegistryResultStatus.BUSY, result.getStatus());

        closeAndAwait(segment);
        removeCacheEntry(segmentId);
    }

    private static void awaitCondition(
            final java.util.function.BooleanSupplier condition,
            final long timeoutMillis) {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                return;
            }
            try {
                Thread.sleep(10L);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void stubSegmentConfig() {
        Mockito.when(conf.getMaxNumberOfKeysInSegmentWriteCache())
                .thenReturn(5);
        Mockito.when(
                conf.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance())
                .thenReturn(6);
        Mockito.when(conf.getMaxNumberOfKeysInSegmentCache()).thenReturn(10);
        Mockito.when(conf.getMaxNumberOfKeysInSegmentChunk()).thenReturn(2);
        Mockito.when(conf.getMaxNumberOfDeltaCacheFiles()).thenReturn(7);
        Mockito.when(conf.getBloomFilterNumberOfHashFunctions()).thenReturn(1);
        Mockito.when(conf.getBloomFilterIndexSizeInBytes()).thenReturn(1024);
        Mockito.when(conf.getBloomFilterProbabilityOfFalsePositive())
                .thenReturn(0.01D);
        Mockito.when(conf.getDiskIoBufferSize()).thenReturn(1024);
        Mockito.when(conf.getEncodingChunkFilters()).thenReturn(FILTERS);
        Mockito.when(conf.getDecodingChunkFilters()).thenReturn(FILTERS);
    }

    private SegmentRegistryStateMachine readGate(
            final SegmentRegistryImpl<Integer, String> target) {
        try {
            final Field field = SegmentRegistryImpl.class
                    .getDeclaredField("gate");
            field.setAccessible(true);
            return (SegmentRegistryStateMachine) field.get(target);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException("Unable to read gate for test", ex);
        }
    }

    private Object createLoadingEntry(final long accessCx) {
        try {
            final Class<?> entryClass = Class.forName(
                    "org.hestiastore.index.segmentregistry.SegmentRegistryCache$Entry");
            final java.lang.reflect.Constructor<?> constructor = entryClass
                    .getDeclaredConstructor(long.class);
            constructor.setAccessible(true);
            return constructor.newInstance(accessCx);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException("Unable to create loading entry",
                    ex);
        }
    }

    private void finishLoad(final Object entry,
            final Segment<Integer, String> segment) {
        try {
            final java.lang.reflect.Method method = entry.getClass()
                    .getDeclaredMethod("finishLoad", Object.class);
            method.setAccessible(true);
            method.invoke(entry, segment);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException("Unable to finish load", ex);
        }
    }

    private boolean invokeTryStartUnload(final Object entry) {
        try {
            final java.lang.reflect.Method method = entry.getClass()
                    .getDeclaredMethod("tryStartUnload",
                            java.util.function.Predicate.class);
            method.setAccessible(true);
            final java.util.function.Predicate<Object> alwaysTrue = value -> true;
            return ((Boolean) method.invoke(entry, alwaysTrue)).booleanValue();
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException("Unable to start unload", ex);
        }
    }

    private void putCacheEntry(final SegmentId segmentId, final Object entry) {
        readCacheMap().put(segmentId, entry);
    }

    private Object getCacheEntry(final SegmentId segmentId) {
        return readCacheMap().get(segmentId);
    }

    private void removeCacheEntry(final SegmentId segmentId) {
        readCacheMap().remove(segmentId);
    }

    @SuppressWarnings("unchecked")
    private Map<SegmentId, Object> readCacheMap() {
        try {
            final Field cacheField = SegmentRegistryImpl.class
                    .getDeclaredField("cache");
            cacheField.setAccessible(true);
            final Object cache = cacheField.get(registry);

            final Field mapField = cache.getClass().getDeclaredField("map");
            mapField.setAccessible(true);
            return (Map<SegmentId, Object>) mapField.get(cache);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException("Unable to read cache map", ex);
        }
    }

    private static final class GateGuard implements AutoCloseable {
        private final SegmentRegistryStateMachine gate;

        private GateGuard(final SegmentRegistryStateMachine gate) {
            this.gate = gate;
        }

        @Override
        public void close() {
            gate.finishFreezeToReady();
        }
    }
}
