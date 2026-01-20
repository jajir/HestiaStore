package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SegmentBuildContextTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(1);
    private static final TypeDescriptorInteger KEY_TYPE_DESCRIPTOR = new TypeDescriptorInteger();
    private static final TypeDescriptorShortString VALUE_TYPE_DESCRIPTOR = new TypeDescriptorShortString();

    @Nested
    class Defaults {

        private SegmentBuildContext<Integer, String> context;

        @BeforeEach
        void setUp() {
            final var asyncDirectory = AsyncDirectoryAdapter
                    .wrap(new MemDirectory());
            final SegmentBuilder<Integer, String> builder = Segment
                    .<Integer, String>builder(asyncDirectory)
                    .withId(SEGMENT_ID)
                    .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)
                    .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)
                    .withMaxNumberOfKeysInSegmentWriteCache(10)
                    .withEncodingChunkFilters(
                            List.of(new ChunkFilterDoNothing()))
                    .withDecodingChunkFilters(
                            List.of(new ChunkFilterDoNothing()));
            context = new SegmentBuildContext<>(builder);
        }

        @AfterEach
        void tearDown() {
            context = null;
        }

        @Test
        void uses_default_maintenance_cache_size() {
            assertEquals(20, context.segmentConf
                    .getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance());
        }

        @Test
        void keeps_bloom_filter_settings_unset() {
            assertFalse(context.segmentConf
                    .hasBloomFilterNumberOfHashFunctions());
            assertFalse(context.segmentConf.hasBloomFilterIndexSizeInBytes());
        }
    }

    @Nested
    class ExplicitValues {

        private SegmentBuildContext<Integer, String> context;

        @BeforeEach
        void setUp() {
            final var asyncDirectory = AsyncDirectoryAdapter
                    .wrap(new MemDirectory());
            final SegmentBuilder<Integer, String> builder = Segment
                    .<Integer, String>builder(asyncDirectory)
                    .withId(SEGMENT_ID)
                    .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)
                    .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)
                    .withMaxNumberOfKeysInSegmentWriteCache(10)
                    .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(25)
                    .withBloomFilterNumberOfHashFunctions(3)
                    .withBloomFilterIndexSizeInBytes(512)
                    .withEncodingChunkFilters(
                            List.of(new ChunkFilterDoNothing()))
                    .withDecodingChunkFilters(
                            List.of(new ChunkFilterDoNothing()));
            context = new SegmentBuildContext<>(builder);
        }

        @AfterEach
        void tearDown() {
            context = null;
        }

        @Test
        void uses_explicit_maintenance_cache_size() {
            assertEquals(25, context.segmentConf
                    .getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance());
        }

        @Test
        void uses_explicit_bloom_filter_settings() {
            assertEquals(3,
                    context.segmentConf.getBloomFilterNumberOfHashFunctions());
            assertEquals(512,
                    context.segmentConf.getBloomFilterIndexSizeInBytes());
        }
    }
}
