package org.hestiastore.index.segmentindex.core.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeConfigPatch;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimePatchResult;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimePatchValidation;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeSettingKey;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeTuningPatch;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeTuningSnapshot;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RuntimeConfigurationImplTest {

    private RuntimeConfigurationImpl runtimeConfiguration;
    private AtomicReference<Map<RuntimeSettingKey, Integer>> appliedLimits;
    private AtomicInteger splitThresholdRescanCount;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        appliedLimits = new AtomicReference<>();
        splitThresholdRescanCount = new AtomicInteger(0);
        runtimeConfiguration = new RuntimeConfigurationImpl(
                RuntimeTuningState.fromConfiguration(conf), appliedLimits::set,
                splitThresholdRescanCount::incrementAndGet);
    }

    @Test
    void validate_rejectsPartitionBufferNotGreaterThanActivePartition() {
        final RuntimeConfigPatch patch = new RuntimeConfigPatch(
                Map.of(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION,
                        Integer.valueOf(8),
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
                        Integer.valueOf(8)),
                false, null);

        final RuntimePatchValidation validation = runtimeConfiguration
                .validate(patch);

        assertFalse(validation.isValid());
        assertEquals(1, validation.getIssues().size());
        assertEquals(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
                validation.getIssues().get(0).key());
    }

    @Test
    void apply_updatesEffectiveLimitsAndTriggersSplitThresholdRescan() {
        final RuntimeConfigPatch patch = new RuntimeConfigPatch(
                Map.of(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
                        Integer.valueOf(42)),
                false, Long.valueOf(0L));

        final RuntimePatchResult result = runtimeConfiguration
                .apply(patch);

        assertTrue(result.isApplied());
        assertNotNull(appliedLimits.get());
        assertEquals(Integer.valueOf(42),
                appliedLimits.get().get(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT));
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
                .legacyImmutableRunLimit(4)
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
        assertEquals(Integer.valueOf(4), rawPatch.getValues().get(
                RuntimeSettingKey.MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION));
        assertEquals(Integer.valueOf(12), rawPatch.getValues().get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION));
        assertEquals(Integer.valueOf(18), rawPatch.getValues().get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER));
        assertEquals(Integer.valueOf(72), rawPatch.getValues()
                .get(RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER));
        assertEquals(Integer.valueOf(144), rawPatch.getValues().get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT));
    }

    @Test
    void runtimeTuningSnapshot_exposesTypedCurrentValues() {
        final RuntimeTuningSnapshot snapshot = runtimeConfiguration
                .getCurrentRuntimeTuning();

        assertEquals("runtime-configuration-test", snapshot.getIndexName());
        assertEquals(0L, snapshot.getRevision());
        assertNotNull(snapshot.getCapturedAt());
        assertEquals(Integer.valueOf(3), snapshot.maxSegmentsInCache());
        assertEquals(Integer.valueOf(10), snapshot.segmentCacheKeyLimit());
        assertEquals(Integer.valueOf(2), snapshot.legacyImmutableRunLimit());
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

        final RuntimePatchResult result = runtimeConfiguration
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

        final RuntimePatchResult result = runtimeConfiguration
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

        final RuntimePatchValidation validation = runtimeConfiguration
                .validateRuntimeTuning(patch);

        assertFalse(validation.isValid());
        assertEquals(1, validation.getIssues().size());
        assertEquals(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
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

        final RuntimePatchResult result = runtimeConfiguration
                .applyRuntimeTuning(patch);

        assertTrue(result.isApplied());
        assertEquals(Integer.valueOf(6), appliedLimits.get().get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION));
        assertEquals(Integer.valueOf(8), appliedLimits.get().get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER));
        assertEquals(Integer.valueOf(16), appliedLimits.get()
                .get(RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER));
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
                .writePath(writePath -> writePath.legacyImmutableRunLimit(2))
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
