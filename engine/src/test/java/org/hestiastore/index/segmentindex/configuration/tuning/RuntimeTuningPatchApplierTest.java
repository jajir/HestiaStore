package org.hestiastore.index.segmentindex.configuration.tuning;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segmentindex.core.split.SplitRuntime;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RuntimeTuningPatchApplierTest {

    private RuntimeTuningPatchApplier<Integer, String> applier;
    private RuntimeTuningState runtimeTuningState;
    private SegmentRegistry<Integer, String> segmentRegistry;
    private SegmentRegistry.Runtime<Integer, String> segmentRuntime;
    private ChunkStoreCache<Integer, String> chunkStoreCache;
    private SplitRuntime<Integer, String> splitService;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        runtimeTuningState = RuntimeTuningState.fromConfiguration(
                effective(buildConf()));
        segmentRegistry = mock(SegmentRegistry.class);
        segmentRuntime = mock(SegmentRegistry.Runtime.class);
        chunkStoreCache = mock(ChunkStoreCache.class);
        splitService = mock(SplitRuntime.class);
        when(segmentRuntime.loadedSegmentsSnapshot()).thenReturn(List.of());
        applier = new RuntimeTuningPatchApplier<>(
                new RuntimeTuningPatchValidator(runtimeTuningState),
                runtimeTuningState,
                new RuntimeSegmentLimitApplier<>(segmentRegistry,
                        segmentRuntime, chunkStoreCache),
                splitService);
    }

    @Test
    void applyReturnsValidationFailureWithoutApplyingLimits() {
        final RuntimeTuningResult result = applier.apply(RuntimeTuningPatch
                .builder()
                .segmentWriteCacheKeyLimit(9)
                .segmentWriteCacheKeyLimitDuringMaintenance(8)
                .build());

        assertFalse(result.applied());
        assertEquals(RuntimeTuningApplyStatus.REJECTED, result.status());
        assertEquals(1, result.validation().issues().size());
        verifyNoInteractions(segmentRegistry, segmentRuntime, chunkStoreCache,
                splitService);
        assertTrue(result.changes().isEmpty());
    }

    @Test
    void validateIsTheDryRunPathAndDoesNotMutateState() {
        final RuntimeTuningPatch patch = RuntimeTuningPatch.builder()
                .expectedRevision(0L)
                .segmentSplitKeyThreshold(42)
                .build();

        final RuntimeTuningValidation validation =
                new RuntimeTuningPatchValidator(runtimeTuningState)
                        .validate(patch);

        assertTrue(validation.valid());
        assertEquals(0L, runtimeTuningState.revision());
        verifyNoInteractions(segmentRegistry, segmentRuntime, chunkStoreCache,
                splitService);
    }

    @Test
    void applyUpdatesEffectiveLimitsAndNotifiesSplitThresholdChanges() {
        final RuntimeTuningResult result = applier.apply(RuntimeTuningPatch
                .builder()
                .expectedRevision(0L)
                .segmentSplitKeyThreshold(42)
                .build());

        assertTrue(result.applied());
        assertNotNull(appliedLimits());
        verify(splitService).requestFullSplitScan();
        assertEquals(1L, result.after().revision());
    }

    @Test
    void applyPublishesValuesToRuntimeStateAccessors() {
        final RuntimeTuningResult result = applier.apply(RuntimeTuningPatch
                .builder()
                .expectedRevision(0L)
                .cachedSegmentLimit(4)
                .cacheKeyLimit(12)
                .segmentWriteCacheKeyLimit(6)
                .segmentWriteCacheKeyLimitDuringMaintenance(8)
                .indexBufferedWriteKeyLimit(16)
                .segmentSplitKeyThreshold(60)
                .chunkStoreCachePageLimit(2)
                .build());

        assertTrue(result.applied());
        assertEquals(1L, runtimeTuningState.revision());
        assertEquals(4, runtimeTuningState.cachedSegmentLimit());
        assertEquals(12, runtimeTuningState.cacheKeyLimit());
        assertEquals(6, runtimeTuningState.segmentWriteCacheKeyLimit());
        assertEquals(8,
                runtimeTuningState.segmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(16, runtimeTuningState.indexBufferedWriteKeyLimit());
        assertEquals(60, runtimeTuningState.segmentSplitKeyThreshold());
        assertEquals(2, runtimeTuningState.chunkStoreCachePageLimit());
    }

    @Test
    void sideEffectFailureDoesNotMutateRuntimeState() {
        doThrow(new IllegalStateException("failed")).when(segmentRegistry)
                .updateCacheLimit(anyInt());
        final RuntimeTuningPatchApplier<Integer, String> failingApplier =
                new RuntimeTuningPatchApplier<>(
                        new RuntimeTuningPatchValidator(runtimeTuningState),
                        runtimeTuningState,
                        new RuntimeSegmentLimitApplier<>(segmentRegistry,
                                segmentRuntime, chunkStoreCache),
                        splitService);

        final RuntimeTuningPatch patch = RuntimeTuningPatch.builder()
                .expectedRevision(0L)
                .segmentSplitKeyThreshold(42)
                .build();

        assertThrows(IllegalStateException.class,
                () -> failingApplier.apply(patch));
        assertEquals(0L, runtimeTuningState.revision());
        assertEquals(50, runtimeTuningState.segmentSplitKeyThreshold());
        verifyNoInteractions(splitService);
    }

    private SegmentRuntimeLimits appliedLimits() {
        final ArgumentCaptor<SegmentRuntimeLimits> captor = ArgumentCaptor
                .forClass(SegmentRuntimeLimits.class);
        verify(segmentRuntime).updateRuntimeLimits(captor.capture());
        return captor.getValue();
    }

    private static IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("runtime-config-patch-applier-test"))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(7))
                .writePath(writePath -> writePath.indexBufferedWriteKeyLimit(9))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .writePath(writePath -> writePath.segmentSplitKeyThreshold(50))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }
}
