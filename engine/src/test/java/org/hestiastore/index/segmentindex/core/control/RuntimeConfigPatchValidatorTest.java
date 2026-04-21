package org.hestiastore.index.segmentindex.core.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchValidation;
import org.hestiastore.index.control.model.RuntimeSettingKey;
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
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("runtime-config-patch-validator-test")
                .withContextLoggingEnabled(false)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInActivePartition(5)
                .withMaxNumberOfImmutableRunsPerPartition(2)
                .withMaxNumberOfKeysInPartitionBuffer(7)
                .withMaxNumberOfKeysInIndexBuffer(9)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withMaxNumberOfKeysInSegment(100)
                .withMaxNumberOfKeysInPartitionBeforeSplit(50)
                .withMaxNumberOfSegmentsInCache(3)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSizeInBytes(1024)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }
}
