package org.hestiastore.index.segmentindex;

import static org.hestiastore.index.segment.SegmentTestHelper.closeAndAwait;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentregistry.SegmentRegistryAccess;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryStateMachine;
import org.hestiastore.index.segmentregistry.SegmentRegistryImpl;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;
import org.hestiastore.index.segmentregistry.SegmentFactory;
import org.hestiastore.index.segmentregistry.SegmentHandlerLockStatus;
import org.hestiastore.index.segmentregistry.SegmentIdAllocator;
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
    private SegmentFactory<Integer, String> segmentFactory;
    private SegmentIdAllocator segmentIdAllocator;

    @BeforeEach
    void setUp() {
        Mockito.when(conf.getMaxNumberOfSegmentsInCache()).thenReturn(3);
        directoryFacade = AsyncDirectoryAdapter.wrap(new MemDirectory());
        maintenanceExecutor = new SegmentAsyncExecutor(1,
                "segment-maintenance");
        segmentFactory = new SegmentFactory<>(directoryFacade, KEY_DESCRIPTOR,
                VALUE_DESCRIPTOR, conf, maintenanceExecutor.getExecutor());
        final AtomicInteger nextId = new AtomicInteger(1);
        segmentIdAllocator = () -> SegmentId.of(nextId.getAndIncrement());
        registry = (SegmentRegistryImpl<Integer, String>) SegmentRegistry
                .<Integer, String>builder().withDirectoryFacade(directoryFacade)
                .withKeyTypeDescriptor(KEY_DESCRIPTOR)
                .withValueTypeDescriptor(VALUE_DESCRIPTOR)
                .withConfiguration(conf)
                .withMaintenanceExecutor(maintenanceExecutor.getExecutor())
                .withSegmentFactory(segmentFactory)
                .withSegmentIdAllocator(segmentIdAllocator).build();
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
        final SegmentId segmentId = SegmentId.of(1);

        final SegmentRegistryAccess<Segment<Integer, String>> firstResult = registry
                .getSegment(segmentId);
        final SegmentRegistryAccess<Segment<Integer, String>> secondResult = registry
                .getSegment(segmentId);
        assertSame(SegmentRegistryResultStatus.OK,
                firstResult.getSegmentStatus());
        assertSame(SegmentRegistryResultStatus.OK,
                secondResult.getSegmentStatus());
        final Segment<Integer, String> first = firstResult.getSegment()
                .orElse(null);
        final Segment<Integer, String> second = secondResult.getSegment()
                .orElse(null);

        assertSame(first, second);
        closeAndAwait(first);
        final Segment<Integer, String> third = registry.getSegment(segmentId)
                .getSegment().orElse(null);

        assertNotSame(first, third);
    }

    @Test
    void getSegment_returnsBusyWhileRegistryFrozen() {
        final SegmentId segmentId = SegmentId.of(1);

        final SegmentRegistryStateMachine gate = readGate(registry);
        assertTrue(gate.tryEnterFreeze());
        try (GateGuard ignored = new GateGuard(gate)) {
            final SegmentRegistryAccess<Segment<Integer, String>> busy = registry
                    .getSegment(segmentId);
            assertSame(SegmentRegistryResultStatus.BUSY,
                    busy.getSegmentStatus());
        }
    }

    @Test
    void getSegment_returnsClosedWhenRegistryClosed() {
        registry.close();

        final SegmentRegistryAccess<Segment<Integer, String>> result = registry
                .getSegment(SegmentId.of(1));

        assertSame(SegmentRegistryResultStatus.CLOSED,
                result.getSegmentStatus());
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
                .withSegmentFactory(segmentFactory)
                .withSegmentIdAllocator(segmentIdAllocator).build();
        stubSegmentConfig();
        final Segment<Integer, String> first = registry.createSegment()
                .getSegment().orElse(null);
        final Segment<Integer, String> second = registry.createSegment()
                .getSegment().orElse(null);
        final Segment<Integer, String> third = registry.createSegment()
                .getSegment().orElse(null);

        final long closedCount = List.of(first, second, third).stream()
                .filter(segment -> segment.getState() == SegmentState.CLOSED)
                .count();

        assertTrue(closedCount >= 1);
    }

    @Test
    void deleteSegment_returnsBusyWhenHandlerLocked() {
        stubSegmentConfig();
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentRegistryAccess<Segment<Integer, String>> access = registry
                .getSegment(segmentId);
        assertSame(SegmentRegistryResultStatus.OK, access.getSegmentStatus());
        assertSame(SegmentHandlerLockStatus.OK, access.lock());

        final SegmentRegistryAccess<Void> result = registry
                .deleteSegment(segmentId);
        assertSame(SegmentRegistryResultStatus.BUSY, result.getSegmentStatus());

        access.unlock();
    }

    @Test
    void getSegment_returnsBusyWhenHandlerLocked() {
        stubSegmentConfig();
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentRegistryAccess<Segment<Integer, String>> access = registry
                .getSegment(segmentId);
        assertSame(SegmentRegistryResultStatus.OK, access.getSegmentStatus());
        assertSame(SegmentHandlerLockStatus.OK, access.lock());

        final SegmentRegistryAccess<Segment<Integer, String>> result = registry
                .getSegment(segmentId);
        assertSame(SegmentRegistryResultStatus.BUSY, result.getSegmentStatus());

        access.unlock();
    }

    @Test
    void getSegment_returnsNotFoundWhenSegmentMissing() {
        stubSegmentConfig();
        final Segment<Integer, String> existing = registry.createSegment()
                .getSegment().orElse(null);
        final SegmentId missingId = SegmentId.of(existing.getId().getId() + 1);

        final SegmentRegistryAccess<Segment<Integer, String>> result = registry
                .getSegment(missingId);

        assertSame(SegmentRegistryResultStatus.NOT_FOUND,
                result.getSegmentStatus());
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
