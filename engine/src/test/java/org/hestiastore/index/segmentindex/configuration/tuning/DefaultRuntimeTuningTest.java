package org.hestiastore.index.segmentindex.configuration.tuning;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStore;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.split.SplitRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class DefaultRuntimeTuningTest {

    private DefaultRuntimeTuning<Integer, String> runtimeTuningService;
    private SegmentRegistry<Integer, String> segmentRegistry;
    private SegmentRegistry.Runtime<Integer, String> segmentRuntime;
    private ChunkStoreCache<Integer, String> chunkStoreCache;
    private SplitRuntime<Integer, String> splitService;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        final EffectiveIndexConfiguration<Integer, String> effective =
                effective(conf);
        segmentRegistry = mock(SegmentRegistry.class);
        segmentRuntime = mock(SegmentRegistry.Runtime.class);
        chunkStoreCache = mock(ChunkStoreCache.class);
        splitService = mock(SplitRuntime.class);
        when(segmentRuntime.loadedSegmentsSnapshot()).thenReturn(List.of());
        runtimeTuningService = new DefaultRuntimeTuning<>(
                RuntimeTuningState.fromConfiguration(effective),
                new RuntimeSegmentLimitApplier<>(segmentRegistry,
                        segmentRuntime, chunkStoreCache),
                splitService, effective,
                new IndexConfigurationStore<>(new MemDirectory()));
    }

    @Test
    void validateRejectsMaintenanceBufferNotGreaterThanActiveBuffer() {
        final RuntimeTuningPatch patch = RuntimeTuningPatch.builder()
                .segmentWriteCacheKeyLimit(8)
                .segmentWriteCacheKeyLimitDuringMaintenance(8)
                .build();

        final RuntimeTuningValidation validation = runtimeTuningService
                .validate(patch);

        assertFalse(validation.valid());
        assertEquals(1, validation.issues().size());
        assertEquals(
                RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE,
                validation.issues().get(0).field());
    }

    @Test
    void applyUpdatesEffectiveLimitsAndTriggersSplitThresholdRescan() {
        final RuntimeTuningPatch patch = RuntimeTuningPatch.builder()
                .expectedRevision(0L)
                .segmentSplitKeyThreshold(42)
                .build();

        final RuntimeTuningResult result = runtimeTuningService.apply(patch);

        assertTrue(result.applied());
        assertEquals(RuntimeTuningApplyStatus.APPLIED, result.status());
        verify(splitService).run();
        assertEquals(0L, result.before().revision());
        assertEquals(1L, result.after().revision());
        assertEquals(List.of(RuntimeTuningKey.SEGMENT_SPLIT_KEY_THRESHOLD),
                result.changes().stream().map(RuntimeTuningChange::field)
                        .toList());
    }

    @Test
    void runtimeTuningPatchCollectsTypedValues() {
        final RuntimeTuningPatch patch = RuntimeTuningPatch.builder()
                .expectedRevision(3L)
                .cachedSegmentLimit(8)
                .cacheKeyLimit(30)
                .segmentWriteCacheKeyLimit(12)
                .segmentWriteCacheKeyLimitDuringMaintenance(18)
                .indexBufferedWriteKeyLimit(72)
                .segmentSplitKeyThreshold(144)
                .build();

        assertEquals(Long.valueOf(3L), patch.expectedRevision());
        assertEquals(RuntimeTuningValue.ofInt(8), patch.values()
                .get(RuntimeTuningKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE));
        assertEquals(RuntimeTuningValue.ofInt(30), patch.values()
                .get(RuntimeTuningKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE));
        assertEquals(RuntimeTuningValue.ofInt(12), patch.values().get(
                RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT));
        assertEquals(RuntimeTuningValue.ofInt(18), patch.values().get(
                RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE));
        assertEquals(RuntimeTuningValue.ofInt(72), patch.values()
                .get(RuntimeTuningKey.INDEX_BUFFERED_WRITE_KEY_LIMIT));
        assertEquals(RuntimeTuningValue.ofInt(144), patch.values().get(
                RuntimeTuningKey.SEGMENT_SPLIT_KEY_THRESHOLD));
    }

    @Test
    void currentExposesTypedPrimitiveValues() {
        final RuntimeTuningSnapshot snapshot = runtimeTuningService.current();

        assertEquals("runtime-configuration-test", snapshot.indexName());
        assertEquals(0L, snapshot.revision());
        assertNotNull(snapshot.capturedAt());
        assertEquals(3, snapshot.segment().cachedSegmentLimit());
        assertEquals(10, snapshot.segment().cacheKeyLimit());
        assertEquals(5, snapshot.writePath().segmentWriteCacheKeyLimit());
        assertEquals(7, snapshot.writePath()
                .segmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(9, snapshot.writePath().indexBufferedWriteKeyLimit());
        assertEquals(50, snapshot.writePath().segmentSplitKeyThreshold());
    }

    @Test
    void validateSupportsDryRunWithoutMutatingState() {
        final RuntimeTuningPatch patch = RuntimeTuningPatch.builder()
                .expectedRevision(0L)
                .segmentSplitKeyThreshold(42)
                .build();

        final RuntimeTuningValidation validation = runtimeTuningService
                .validate(patch);

        assertTrue(validation.valid());
        verify(splitService, never()).requestFullSplitScan();
        assertEquals(0L, runtimeTuningService.current().revision());
    }

    @Test
    void applyRejectsExpectedRevisionMismatch() {
        final RuntimeTuningPatch patch = RuntimeTuningPatch.builder()
                .expectedRevision(99L)
                .segmentSplitKeyThreshold(42)
                .build();

        final RuntimeTuningResult result = runtimeTuningService.apply(patch);

        assertFalse(result.applied());
        assertEquals(RuntimeTuningApplyStatus.REJECTED, result.status());
        assertEquals(1, result.validation().issues().size());
        assertEquals("expectedRevision does not match current revision",
                result.validation().issues().get(0).message());
        assertSame(result.before(), result.after());
    }

    @Test
    void applyUpdatesMultipleEffectiveLimitsAtomically() {
        final RuntimeTuningPatch patch = RuntimeTuningPatch.builder()
                .expectedRevision(0L)
                .segmentWriteCacheKeyLimit(6)
                .segmentWriteCacheKeyLimitDuringMaintenance(8)
                .indexBufferedWriteKeyLimit(16)
                .build();

        final RuntimeTuningResult result = runtimeTuningService.apply(patch);

        assertTrue(result.applied());
        final SegmentRuntimeLimits limits = appliedLimits();
        assertEquals(6, limits.maxNumberOfKeysInSegmentWriteCache());
        assertEquals(8,
                limits.maxNumberOfKeysInSegmentWriteCacheDuringMaintenance());
        assertEquals(3, result.changes().size());
        final RuntimeTuningChange activeLimitChange = result.changes().stream()
                .filter(change -> change.field() == RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT)
                .findFirst().orElseThrow();
        assertEquals("writePath.segmentWriteCacheKeyLimit",
                activeLimitChange.field().path());
        assertEquals(RuntimeTuningValue.ofInt(5), activeLimitChange.before());
        assertEquals(RuntimeTuningValue.ofInt(6), activeLimitChange.after());
    }

    @Test
    void validNoOpApplyIncrementsRevisionAndHasNoChanges() {
        final RuntimeTuningPatch patch = RuntimeTuningPatch.builder()
                .expectedRevision(0L)
                .cacheKeyLimit(10)
                .build();

        final RuntimeTuningResult result = runtimeTuningService.apply(patch);

        assertTrue(result.applied());
        assertEquals(0L, result.before().revision());
        assertEquals(1L, result.after().revision());
        assertTrue(result.changes().isEmpty());
        verify(splitService, never()).requestFullSplitScan();
    }

    private SegmentRuntimeLimits appliedLimits() {
        final ArgumentCaptor<SegmentRuntimeLimits> captor = ArgumentCaptor
                .forClass(SegmentRuntimeLimits.class);
        verify(segmentRuntime).updateRuntimeLimits(captor.capture());
        return captor.getValue();
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("runtime-configuration-test"))
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
