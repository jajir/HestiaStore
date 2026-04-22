package org.hestiastore.index.segmentregistry;

import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.OperationResult;
import static org.hestiastore.index.segment.SegmentTestHelper.closeAndAssertClosed;
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
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
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
    private Directory directoryFacade;

    @Mock
    private IndexConfiguration<Integer, String> conf;

    @Mock
    private IndexRuntimeConfiguration<Integer, String> runtimeConfiguration;

    private SegmentRegistryImpl<Integer, String> registry;
    private ExecutorService stableSegmentMaintenancePool;
    private ExecutorService registryMaintenancePool;
    private List<Supplier<? extends ChunkFilter>> filterSuppliers;

    @BeforeEach
    void setUp() {
        Mockito.when(conf.getMaxNumberOfSegmentsInCache()).thenReturn(3);
        Mockito.when(conf.resolveRuntimeConfiguration())
                .thenReturn(runtimeConfiguration);
        directoryFacade = new MemDirectory();
        stableSegmentMaintenancePool = Executors.newSingleThreadExecutor();
        rebuildRegistry();
    }

    @AfterEach
    void tearDown() {
        closeRegistry();
        if (stableSegmentMaintenancePool != null) {
            stableSegmentMaintenancePool.shutdownNow();
            stableSegmentMaintenancePool = null;
        }
    }

    @Test
    void getSegment_reusesInstanceUntilClosed() {
        stubSegmentConfig();
        final Segment<Integer, String> created = registry.tryCreateSegment()
                .getValue();
        final SegmentId segmentId = created.getId();

        final Segment<Integer, String> first = registry.tryLoadSegment(segmentId)
                .getValue();
        final Segment<Integer, String> second = registry.tryLoadSegment(segmentId)
                .getValue();

        assertSame(first, second);
        closeAndAssertClosed(first);
        final Segment<Integer, String> third = registry.tryLoadSegment(segmentId)
                .getValue();

        assertNotSame(first, third);
    }

    @Test
    void registryStartupTransitionsGateToReady() {
        final SegmentRegistryStateMachine gate = readGate(registry);
        assertSame(SegmentRegistryState.READY, gate.getState());
    }

    @Test
    void getSegment_returnsBusyWhileRegistryFrozen() {
        final SegmentId segmentId = SegmentId.of(1);

        final SegmentRegistryStateMachine gate = readGate(registry);
        assertTrue(gate.tryEnterFreeze());
        try (GateGuard ignored = new GateGuard(gate)) {
            final OperationResult<Segment<Integer, String>> result = registry
                    .tryLoadSegment(segmentId);
            assertSame(OperationStatus.BUSY, result.getStatus());
        }
    }

    @Test
    void getSegment_returnsClosedWhenRegistryClosed() {
        registry.close();

        final OperationResult<Segment<Integer, String>> result = registry
                .tryLoadSegment(SegmentId.of(1));
        assertSame(OperationStatus.CLOSED, result.getStatus());
    }

    @Test
    void getSegment_returnsErrorWhenRegistryIsInErrorState() {
        final SegmentRegistryStateMachine gate = readGate(registry);
        gate.fail();

        final OperationResult<Segment<Integer, String>> result = registry
                .tryLoadSegment(SegmentId.of(1));
        assertSame(OperationStatus.ERROR, result.getStatus());
    }

    @Test
    void getSegment_evicts_least_recently_used_when_limit_exceeded() {
        Mockito.when(conf.getMaxNumberOfSegmentsInCache()).thenReturn(2);
        rebuildRegistry();
        stubSegmentConfig();
        final Segment<Integer, String> first = registry.tryCreateSegment()
                .getValue();
        final Segment<Integer, String> second = registry.tryCreateSegment()
                .getValue();
        final Segment<Integer, String> third = registry.tryCreateSegment()
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
    void pinSegment_preventsEvictionWhileCacheIsOverLimit() {
        Mockito.when(conf.getMaxNumberOfSegmentsInCache()).thenReturn(2);
        rebuildRegistry();
        stubSegmentConfig();
        final Segment<Integer, String> first = registry.tryCreateSegment()
                .getValue();
        final Segment<Integer, String> second = registry.tryCreateSegment()
                .getValue();
        invokePinSegment(registry, first.getId());

        final Segment<Integer, String> third = registry.tryCreateSegment()
                .getValue();

        awaitCondition(() -> List.of(second, third).stream().anyMatch(
                segment -> segment.getState() == SegmentState.CLOSED), 1500L);

        assertNotSame(SegmentState.CLOSED, first.getState(),
                "Pinned segment should stay loaded while peers are evicted");
        assertTrue(List.of(second, third).stream().anyMatch(
                segment -> segment.getState() == SegmentState.CLOSED));

        invokeUnpinSegment(registry, first.getId());
    }

    @Test
    void getSegment_returnsBusyWhenSegmentMissing() {
        stubSegmentConfig();
        final Segment<Integer, String> existing = registry.tryCreateSegment()
                .getValue();
        final SegmentId missingId = SegmentId.of(existing.getId().getId() + 1);

        final OperationResult<Segment<Integer, String>> result = registry
                .tryLoadSegment(missingId);
        assertSame(OperationStatus.BUSY, result.getStatus());
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
            final Future<OperationResult<Segment<Integer, String>>> waiter = callers
                    .submit(() -> registry.tryLoadSegment(segmentId));

            org.junit.jupiter.api.Assertions.assertThrows(
                    TimeoutException.class,
                    () -> waiter.get(100, TimeUnit.MILLISECONDS));

            finishLoad(entry, segment);

            final OperationResult<Segment<Integer, String>> result = waiter
                    .get(1, TimeUnit.SECONDS);
            assertSame(OperationStatus.OK, result.getStatus());
            assertSame(segment, result.getValue());
        } finally {
            callers.shutdownNow();
            removeCacheEntry(segmentId);
        }
    }

    @Test
    void getSegment_returnsBusyWhenEntryIsUnloading() {
        stubSegmentConfig();
        final Segment<Integer, String> created = registry.tryCreateSegment()
                .getValue();
        final SegmentId segmentId = created.getId();
        final Segment<Integer, String> segment = registry.tryLoadSegment(segmentId)
                .getValue();
        assertNotNull(segment);

        final Object entry = getCacheEntry(segmentId);
        assertTrue(invokeTryStartUnload(entry));

        final OperationResult<Segment<Integer, String>> result = registry
                .tryLoadSegment(segmentId);
        assertSame(OperationStatus.BUSY, result.getStatus());

        closeAndAssertClosed(segment);
        removeCacheEntry(segmentId);
    }

    private void rebuildRegistry() {
        closeRegistry();
        registryMaintenancePool = Executors.newSingleThreadExecutor();
        registry = (SegmentRegistryImpl<Integer, String>) SegmentRegistry
                .<Integer, String>builder().withDirectoryFacade(directoryFacade)
                .withKeyTypeDescriptor(KEY_DESCRIPTOR)
                .withValueTypeDescriptor(VALUE_DESCRIPTOR)
                .withConfiguration(conf)
                .withSegmentMaintenanceExecutor(stableSegmentMaintenancePool)
                .withRegistryMaintenanceExecutor(registryMaintenancePool)
                .build();
    }

    private void closeRegistry() {
        if (registry != null) {
            try {
                registry.close();
            } catch (final IndexException ex) {
                // Tests may intentionally leave the registry in ERROR state.
            }
            registry = null;
        }
        if (registryMaintenancePool != null) {
            registryMaintenancePool.shutdownNow();
            registryMaintenancePool = null;
        }
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
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
        }
    }

    private void stubSegmentConfig() {
        filterSuppliers = List.of(ChunkFilterDoNothing::new);
        Mockito.when(conf.getMaxNumberOfKeysInActivePartition())
                .thenReturn(5);
        Mockito.when(
                conf.getMaxNumberOfKeysInPartitionBuffer())
                .thenReturn(6);
        Mockito.when(conf.getMaxNumberOfKeysInSegmentCache()).thenReturn(10);
        Mockito.when(conf.getMaxNumberOfKeysInSegmentChunk()).thenReturn(2);
        Mockito.when(conf.getMaxNumberOfDeltaCacheFiles()).thenReturn(7);
        Mockito.when(conf.getBloomFilterNumberOfHashFunctions()).thenReturn(1);
        Mockito.when(conf.getBloomFilterIndexSizeInBytes()).thenReturn(1024);
        Mockito.when(conf.getBloomFilterProbabilityOfFalsePositive())
                .thenReturn(0.01D);
        Mockito.when(conf.getDiskIoBufferSize()).thenReturn(1024);
        Mockito.when(runtimeConfiguration.getEncodingChunkFilterSuppliers())
                .thenReturn(filterSuppliers);
        Mockito.when(runtimeConfiguration.getDecodingChunkFilterSuppliers())
                .thenReturn(filterSuppliers);
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

    private static void invokePinSegment(
            final SegmentRegistryImpl<Integer, String> registry,
            final SegmentId segmentId) {
        invokeRegistryMethod(registry, "pinSegment", segmentId);
    }

    private static void invokeUnpinSegment(
            final SegmentRegistryImpl<Integer, String> registry,
            final SegmentId segmentId) {
        invokeRegistryMethod(registry, "unpinSegment", segmentId);
    }

    private static void invokeRegistryMethod(
            final SegmentRegistryImpl<Integer, String> registry,
            final String methodName, final SegmentId segmentId) {
        try {
            final var method = SegmentRegistryImpl.class.getDeclaredMethod(
                    methodName, SegmentId.class);
            method.setAccessible(true);
            method.invoke(registry, segmentId);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to invoke SegmentRegistryImpl." + methodName, ex);
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
            final Object cache = cacheField.get(registry).orElse(null);

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
