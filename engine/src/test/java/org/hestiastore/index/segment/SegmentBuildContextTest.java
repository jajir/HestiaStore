package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
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
            final var asyncDirectory = new MemDirectory();
            final SegmentBuilder<Integer, String> builder = Segment
                    .<Integer, String>builder(asyncDirectory).withId(SEGMENT_ID)
                    .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)
                    .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)
                    .withMaxNumberOfKeysInSegmentWriteCache(10)
                    .withEncodingChunkFilters(
                            List.of(new ChunkFilterDoNothing()))
                    .withDecodingChunkFilters(
                            List.of(new ChunkFilterDoNothing()));
            context = new SegmentBuildContext<>(builder,
                    new SegmentDirectoryLayout(SEGMENT_ID));
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
            assertFalse(
                    context.segmentConf.hasBloomFilterNumberOfHashFunctions());
            assertFalse(context.segmentConf.hasBloomFilterIndexSizeInBytes());
        }
    }

    @Nested
    class ExplicitValues {

        private SegmentBuildContext<Integer, String> context;

        @BeforeEach
        void setUp() {
            final var asyncDirectory = new MemDirectory();
            final SegmentBuilder<Integer, String> builder = Segment
                    .<Integer, String>builder(asyncDirectory).withId(SEGMENT_ID)
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
            context = new SegmentBuildContext<>(builder,
                    new SegmentDirectoryLayout(SEGMENT_ID));
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

    @Test
    void supplierBasedFiltersArePropagatedToSegmentConfAndSegmentFiles() {
        final AtomicInteger sequence = new AtomicInteger();
        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder(new MemDirectory()).withId(SEGMENT_ID)
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)
                .withMaxNumberOfKeysInSegmentWriteCache(10)
                .withEncodingChunkFilterSuppliers(List.of(
                        () -> new TrackingChunkFilter(sequence.incrementAndGet())))
                .withDecodingChunkFilterSuppliers(List.of(
                        () -> new TrackingChunkFilter(sequence.incrementAndGet())));
        final SegmentBuildContext<Integer, String> context = new SegmentBuildContext<>(
                builder, new SegmentDirectoryLayout(SEGMENT_ID));

        final TrackingChunkFilter firstConfFilter = (TrackingChunkFilter) context.segmentConf
                .getEncodingChunkFilters().get(0);
        final TrackingChunkFilter secondConfFilter = (TrackingChunkFilter) context.segmentConf
                .getEncodingChunkFilters().get(0);
        final TrackingChunkFilter fileFilter = (TrackingChunkFilter) context.segmentFiles
                .getEncodingChunkFilters().get(0);

        assertEquals(1, firstConfFilter.getId());
        assertEquals(2, secondConfFilter.getId());
        assertEquals(3, fileFilter.getId());
        assertNotSame(firstConfFilter, secondConfFilter);
    }

    private static final class TrackingChunkFilter
            implements org.hestiastore.index.chunkstore.ChunkFilter {

        private final int id;

        private TrackingChunkFilter(final int id) {
            this.id = id;
        }

        private int getId() {
            return id;
        }

        @Override
        public ChunkData apply(final ChunkData input) {
            return input;
        }
    }
}
