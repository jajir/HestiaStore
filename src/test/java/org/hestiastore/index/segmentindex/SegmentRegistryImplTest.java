package org.hestiastore.index.segmentindex;

import static org.hestiastore.index.segment.SegmentTestHelper.closeAndAwait;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
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
import org.hestiastore.index.segmentregistry.SegmentRegistryCache;
import org.hestiastore.index.segmentregistry.SegmentRegistryGate;
import org.hestiastore.index.segmentregistry.SegmentRegistryImpl;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;
import org.hestiastore.index.segmentregistry.SegmentRegistryState;
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

    @Mock
    private Segment<Integer, String> oldSegment;

    @Mock
    private Segment<Integer, String> lowerSegment;

    @Mock
    private Segment<Integer, String> upperSegment;

    private SegmentRegistryImpl<Integer, String> registry;

    @BeforeEach
    void setUp() {
        Mockito.when(conf.getNumberOfSegmentIndexMaintenanceThreads())
                .thenReturn(1);
        Mockito.when(conf.getNumberOfIndexMaintenanceThreads()).thenReturn(1);
        Mockito.when(conf.getMaxNumberOfSegmentsInCache()).thenReturn(3);
        directoryFacade = AsyncDirectoryAdapter.wrap(new MemDirectory());
        registry = new SegmentRegistryImpl<>(directoryFacade, KEY_DESCRIPTOR,
                VALUE_DESCRIPTOR, conf);
    }

    @AfterEach
    void tearDown() {
        if (registry != null) {
            registry.close();
        }
    }

    @Test
    void getSegment_reusesInstanceUntilClosed() {
        stubSegmentConfig();
        final SegmentId segmentId = SegmentId.of(1);

        final SegmentRegistryResult<Segment<Integer, String>> firstResult = registry
                .getSegment(segmentId);
        final SegmentRegistryResult<Segment<Integer, String>> secondResult = registry
                .getSegment(segmentId);
        assertSame(SegmentRegistryResultStatus.OK, firstResult.getStatus());
        assertSame(SegmentRegistryResultStatus.OK, secondResult.getStatus());
        final Segment<Integer, String> first = firstResult.getValue();
        final Segment<Integer, String> second = secondResult.getValue();

        assertSame(first, second);
        closeAndAwait(first);
        final Segment<Integer, String> third = registry.getSegment(segmentId)
                .getValue();

        assertNotSame(first, third);
    }

    @Test
    void evictSegmentIfSame_removesOnlyMatchingInstance() {
        stubSegmentConfig();
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentId otherId = SegmentId.of(2);

        final Segment<Integer, String> segment = registry.getSegment(segmentId)
                .getValue();
        final Segment<Integer, String> otherSegment = registry
                .getSegment(otherId).getValue();

        assertFalse(registry.evictSegmentIfSame(segmentId, otherSegment));
        assertSame(segment, registry.getSegment(segmentId).getValue());

        assertTrue(registry.evictSegmentIfSame(segmentId, segment));
        assertNotSame(segment, registry.getSegment(segmentId).getValue());
    }

    void getSegment_returnsBusyWhileRegistryFrozen() {
        final SegmentId segmentId = SegmentId.of(1);

        final SegmentRegistryGate gate = readGate(registry);
        assertTrue(gate.tryEnterFreeze());
        try (GateGuard ignored = new GateGuard(gate)) {
            final SegmentRegistryResult<Segment<Integer, String>> busy = registry
                    .getSegment(segmentId);
            assertSame(SegmentRegistryResultStatus.BUSY, busy.getStatus());
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
    void getSegment_evicts_least_recently_used_when_limit_exceeded() {
        Mockito.when(conf.getMaxNumberOfSegmentsInCache()).thenReturn(2);
        registry.close();
        registry = new SegmentRegistryImpl<>(directoryFacade, KEY_DESCRIPTOR,
                VALUE_DESCRIPTOR, conf);
        stubSegmentConfig();
        final SegmentId firstId = SegmentId.of(1);
        final SegmentId secondId = SegmentId.of(2);
        final SegmentId thirdId = SegmentId.of(3);

        final Segment<Integer, String> first = registry.getSegment(firstId)
                .getValue();
        registry.getSegment(secondId);
        registry.getSegment(thirdId);

        assertEquals(SegmentState.CLOSED, first.getState());
        final Segment<Integer, String> firstReloaded = registry
                .getSegment(firstId).getValue();
        assertNotSame(first, firstReloaded);
    }

    @Test
    void applySplitPlan_removes_old_and_adds_new_segments() {
        final SegmentId oldId = SegmentId.of(1);
        final SegmentId lowerId = SegmentId.of(2);
        final SegmentId upperId = SegmentId.of(3);
        final SegmentSplitApplyPlan<Integer, String> plan = new SegmentSplitApplyPlan<>(
                oldId, lowerId, upperId, 1, 10,
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);
        Mockito.when(lowerSegment.getId()).thenReturn(lowerId);
        Mockito.when(lowerSegment.getState()).thenReturn(SegmentState.CLOSED);
        Mockito.when(upperSegment.getId()).thenReturn(upperId);
        Mockito.when(upperSegment.getState()).thenReturn(SegmentState.CLOSED);
        final SegmentRegistryCache<Integer, String> cache = readCache(
                registry);
        cache.withLock(() -> cache.putLocked(oldId, oldSegment));

        final SegmentRegistryResult<Segment<Integer, String>> result = registry
                .applySplitPlan(plan, lowerSegment, upperSegment);

        assertSame(SegmentRegistryResultStatus.OK, result.getStatus());
        assertSame(oldSegment, result.getValue());
        cache.withLock(() -> {
            assertNull(cache.getLocked(oldId));
            assertSame(lowerSegment, cache.getLocked(lowerId));
            assertSame(upperSegment, cache.getLocked(upperId));
        });
        assertSame(SegmentRegistryState.READY, readGate(registry).getState());
    }

    @Test
    void applySplitPlan_returns_busy_when_registry_frozen() {
        final SegmentId oldId = SegmentId.of(1);
        final SegmentId lowerId = SegmentId.of(2);
        final SegmentId upperId = SegmentId.of(3);
        final SegmentSplitApplyPlan<Integer, String> plan = new SegmentSplitApplyPlan<>(
                oldId, lowerId, upperId, 1, 10,
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);
        Mockito.when(oldSegment.getState()).thenReturn(SegmentState.CLOSED);
        Mockito.when(lowerSegment.getId()).thenReturn(lowerId);
        Mockito.when(upperSegment.getId()).thenReturn(upperId);
        final SegmentRegistryCache<Integer, String> cache = readCache(
                registry);
        cache.withLock(() -> cache.putLocked(oldId, oldSegment));
        final SegmentRegistryGate gate = readGate(registry);

        assertTrue(gate.tryEnterFreeze());
        try (GateGuard ignored = new GateGuard(gate)) {
            final SegmentRegistryResult<Segment<Integer, String>> result = registry
                    .applySplitPlan(plan, lowerSegment, upperSegment);
            assertSame(SegmentRegistryResultStatus.BUSY, result.getStatus());
        }

        cache.withLock(() -> {
            assertSame(oldSegment, cache.getLocked(oldId));
            assertNull(cache.getLocked(lowerId));
            assertNull(cache.getLocked(upperId));
        });
    }

    private void stubSegmentConfig() {
        Mockito.when(conf.getMaxNumberOfKeysInSegmentWriteCache())
                .thenReturn(5);
        Mockito.when(conf.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance())
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

    private SegmentRegistryGate readGate(
            final SegmentRegistryImpl<Integer, String> target) {
        try {
            final Field field = SegmentRegistryImpl.class
                    .getDeclaredField("gate");
            field.setAccessible(true);
            return (SegmentRegistryGate) field.get(target);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read gate for test", ex);
        }
    }

    private SegmentRegistryCache<Integer, String> readCache(
            final SegmentRegistryImpl<Integer, String> target) {
        try {
            final Field field = SegmentRegistryImpl.class
                    .getDeclaredField("cache");
            field.setAccessible(true);
            return (SegmentRegistryCache<Integer, String>) field.get(target);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read cache for test", ex);
        }
    }

    private static final class GateGuard implements AutoCloseable {
        private final SegmentRegistryGate gate;

        private GateGuard(final SegmentRegistryGate gate) {
            this.gate = gate;
        }

        @Override
        public void close() {
            gate.finishFreezeToReady();
        }
    }
}
