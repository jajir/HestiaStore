package org.hestiastore.index.segmentindex.core.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeConfigPatch;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimePatchValidation;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeSettingKey;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RuntimeConfigPatchValidatorTest {

    private RuntimeConfigPatchValidator validator;

    @BeforeEach
    void setUp() {
        validator = new RuntimeConfigPatchValidator(
                RuntimeTuningState.fromConfiguration(buildConf()));
    }

    @Test
    void validateRejectsNullPatch() {
        final RuntimePatchValidation validation = validator.validate(null);

        assertFalse(validation.isValid());
        assertEquals(1, validation.getIssues().size());
        assertEquals("patch must not be null",
                validation.getIssues().get(0).message());
    }

    @Test
    void validateRejectsTooSmallSegmentCacheValue() {
        final RuntimePatchValidation validation = validator
                .validate(new RuntimeConfigPatch(
                        Map.of(
                                RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                                Integer.valueOf(2)),
                        false, null));

        assertFalse(validation.isValid());
        assertEquals(
                RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                validation.getIssues().get(0).key());
    }

    @Test
    void validateNormalizesAcceptedValues() {
        final RuntimePatchValidation validation = validator
                .validate(new RuntimeConfigPatch(
                        Map.of(
                                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION,
                                Integer.valueOf(4),
                                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
                                Integer.valueOf(6),
                                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER,
                                Integer.valueOf(8)),
                        false, null));

        assertTrue(validation.isValid());
        assertEquals(Integer.valueOf(4), validation.getNormalizedValues().get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION));
        assertEquals(Integer.valueOf(6), validation.getNormalizedValues().get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER));
    }

    private static IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("runtime-config-patch-validator-test"))
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
