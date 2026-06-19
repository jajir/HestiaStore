package org.hestiastore.index.segmentindex.configuration.tuning;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RuntimeTuningPatchValidatorTest {

    private RuntimeTuningPatchValidator validator;

    @BeforeEach
    void setUp() {
        validator = new RuntimeTuningPatchValidator(
                RuntimeTuningState.fromConfiguration(effective(buildConf())));
    }

    @Test
    void validateRejectsNullPatch() {
        final RuntimeTuningValidation validation = validator.validate(null);

        assertFalse(validation.valid());
        assertEquals(1, validation.issues().size());
        assertEquals("patch must not be null",
                validation.issues().get(0).message());
    }

    @Test
    void validateRejectsTooSmallSegmentCacheValue() {
        final RuntimeTuningValidation validation = validator
                .validate(RuntimeTuningPatch.builder()
                        .cachedSegmentLimit(2)
                        .build());

        assertFalse(validation.valid());
        assertEquals(RuntimeTuningKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                validation.issues().get(0).field());
    }

    @Test
    void validateNormalizesAcceptedValues() {
        final RuntimeTuningValidation validation = validator
                .validate(RuntimeTuningPatch.builder()
                        .segmentWriteCacheKeyLimit(4)
                        .segmentWriteCacheKeyLimitDuringMaintenance(6)
                        .indexBufferedWriteKeyLimit(8)
                        .build());

        assertTrue(validation.valid());
        assertEquals(RuntimeTuningValue.ofInt(4),
                validation.normalizedValues().get(
                        RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT));
        assertEquals(RuntimeTuningValue.ofInt(6),
                validation.normalizedValues().get(
                        RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE));
    }

    @Test
    void validateAcceptsZeroChunkStoreCachePageLimit() {
        final RuntimeTuningValidation validation = validator
                .validate(RuntimeTuningPatch.builder()
                        .chunkStoreCachePageLimit(0)
                        .build());

        assertTrue(validation.valid());
        assertEquals(RuntimeTuningValue.ofInt(0),
                validation.normalizedValues().get(
                        RuntimeTuningKey.CHUNK_STORE_CACHE_PAGE_LIMIT));
    }

    @Test
    void validateRejectsNegativeChunkStoreCachePageLimit() {
        final RuntimeTuningValidation validation = validator
                .validate(RuntimeTuningPatch.builder()
                        .chunkStoreCachePageLimit(-1)
                        .build());

        assertFalse(validation.valid());
        assertEquals(RuntimeTuningKey.CHUNK_STORE_CACHE_PAGE_LIMIT,
                validation.issues().get(0).field());
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
