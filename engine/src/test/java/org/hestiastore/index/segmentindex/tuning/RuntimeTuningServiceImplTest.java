package org.hestiastore.index.segmentindex.tuning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RuntimeTuningServiceImplTest {

    private RuntimeTuningServiceImpl runtimeTuningService;
    private AtomicReference<Map<RuntimeSettingKey, Integer>> appliedLimits;
    private AtomicInteger splitThresholdRescanCount;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        appliedLimits = new AtomicReference<>();
        splitThresholdRescanCount = new AtomicInteger(0);
        runtimeTuningService = new RuntimeTuningServiceImpl(
                RuntimeTuningState.fromConfiguration(conf), appliedLimits::set,
                splitThresholdRescanCount::incrementAndGet);
    }

    @Test
    void validate_rejectsPartitionBufferNotGreaterThanActivePartition() {
        final RuntimeConfigPatch patch = new RuntimeConfigPatch(
                Map.of(
                        RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT,
                        Integer.valueOf(8),
                        RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE,
                        Integer.valueOf(8)),
                false, null);

        final RuntimePatchValidation validation = runtimeTuningService
                .validate(patch);

        assertFalse(validation.isValid());
        assertEquals(1, validation.getIssues().size());
        assertEquals(
                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE,
                validation.getIssues().get(0).key());
    }

    @Test
    void apply_updatesEffectiveLimitsAndTriggersSplitThresholdRescan() {
        final RuntimeConfigPatch patch = new RuntimeConfigPatch(
                Map.of(
                        RuntimeSettingKey.SEGMENT_SPLIT_KEY_THRESHOLD,
                        Integer.valueOf(42)),
                false, Long.valueOf(0L));

        final RuntimePatchResult result = runtimeTuningService
                .apply(patch);

        assertTrue(result.isApplied());
        assertNotNull(appliedLimits.get());
        assertEquals(Integer.valueOf(42),
                appliedLimits.get().get(
                        RuntimeSettingKey.SEGMENT_SPLIT_KEY_THRESHOLD));
        assertEquals(1, splitThresholdRescanCount.get());
        assertEquals(1L, result.getSnapshot().revision());
    }

    @Test
    void runtimeTuningPatch_convertsToRuntimeConfigPatch() {
        final RuntimeTuningPatch patch = RuntimeTuningPatch.builder()
                .expectedRevision(3L)
                .dryRun()
                .maxSegmentsInCache(8)
                .segmentCacheKeyLimit(30)
                .writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(12)
                        .maintenanceWriteCacheKeyLimit(18)
                        .indexBufferedWriteKeyLimit(72)
                        .segmentSplitKeyThreshold(144))
                .build();

        final RuntimeConfigPatch rawPatch = patch.toRuntimeConfigPatch();

        assertTrue(rawPatch.isDryRun());
        assertEquals(Long.valueOf(3L), rawPatch.getExpectedRevision());
        assertEquals(Integer.valueOf(8), rawPatch.getValues()
                .get(RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE));
        assertEquals(Integer.valueOf(30), rawPatch.getValues()
                .get(RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE));
        assertEquals(Integer.valueOf(12), rawPatch.getValues().get(
                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT));
        assertEquals(Integer.valueOf(18), rawPatch.getValues().get(
                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE));
        assertEquals(Integer.valueOf(72), rawPatch.getValues()
                .get(RuntimeSettingKey.INDEX_BUFFERED_WRITE_KEY_LIMIT));
        assertEquals(Integer.valueOf(144), rawPatch.getValues().get(
                RuntimeSettingKey.SEGMENT_SPLIT_KEY_THRESHOLD));
    }

    @Test
    void runtimeTuningSnapshot_exposesTypedCurrentValues() {
        final RuntimeTuningSnapshot snapshot = runtimeTuningService
                .getCurrentRuntimeTuning();

        assertEquals("runtime-configuration-test", snapshot.getIndexName());
        assertEquals(0L, snapshot.getRevision());
        assertNotNull(snapshot.getCapturedAt());
        assertEquals(Integer.valueOf(3), snapshot.maxSegmentsInCache());
        assertEquals(Integer.valueOf(10), snapshot.segmentCacheKeyLimit());
        assertEquals(Integer.valueOf(5),
                snapshot.writePath().segmentWriteCacheKeyLimit());
        assertEquals(Integer.valueOf(7),
                snapshot.writePath().maintenanceWriteCacheKeyLimit());
        assertEquals(Integer.valueOf(9),
                snapshot.writePath().indexBufferedWriteKeyLimit());
        assertEquals(Integer.valueOf(50),
                snapshot.writePath().segmentSplitKeyThreshold());
    }

    @Test
    void applyRuntimeTuning_supportsDryRunWithoutMutatingState() {
        final RuntimeTuningPatch patch = RuntimeTuningPatch.builder()
                .expectedRevision(0L)
                .dryRun()
                .segmentSplitKeyThreshold(42)
                .build();

        final RuntimePatchResult result = runtimeTuningService
                .applyRuntimeTuning(patch);

        assertFalse(result.isApplied());
        assertEquals(0, splitThresholdRescanCount.get());
        assertEquals(0L, result.getSnapshot().revision());
    }

    @Test
    void applyRuntimeTuning_rejectsExpectedRevisionMismatch() {
        final RuntimeTuningPatch patch = RuntimeTuningPatch.builder()
                .expectedRevision(99L)
                .segmentSplitKeyThreshold(42)
                .build();

        final RuntimePatchResult result = runtimeTuningService
                .applyRuntimeTuning(patch);

        assertFalse(result.isApplied());
        assertEquals(1, result.getValidation().getIssues().size());
        assertEquals("expectedRevision does not match current revision",
                result.getValidation().getIssues().get(0).message());
    }

    @Test
    void validateRuntimeTuning_rejectsInvalidWritePathLimits() {
        final RuntimeTuningPatch patch = RuntimeTuningPatch.builder()
                .writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(8)
                        .maintenanceWriteCacheKeyLimit(8))
                .build();

        final RuntimePatchValidation validation = runtimeTuningService
                .validateRuntimeTuning(patch);

        assertFalse(validation.isValid());
        assertEquals(1, validation.getIssues().size());
        assertEquals(
                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE,
                validation.getIssues().get(0).key());
    }

    @Test
    void applyRuntimeTuning_updatesEffectiveLimits() {
        final RuntimeTuningPatch patch = RuntimeTuningPatch.builder()
                .expectedRevision(0L)
                .writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(6)
                        .maintenanceWriteCacheKeyLimit(8)
                        .indexBufferedWriteKeyLimit(16))
                .build();

        final RuntimePatchResult result = runtimeTuningService
                .applyRuntimeTuning(patch);

        assertTrue(result.isApplied());
        assertEquals(Integer.valueOf(6), appliedLimits.get().get(
                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT));
        assertEquals(Integer.valueOf(8), appliedLimits.get().get(
                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE));
        assertEquals(Integer.valueOf(16), appliedLimits.get()
                .get(RuntimeSettingKey.INDEX_BUFFERED_WRITE_KEY_LIMIT));
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
