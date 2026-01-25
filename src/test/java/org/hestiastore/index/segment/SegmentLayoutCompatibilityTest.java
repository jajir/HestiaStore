package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hestiastore.index.segment.SegmentTestHelper.closeAndAwait;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SegmentLayoutCompatibilityTest extends AbstractSegmentTest {

    private final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
    private final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void segment_roundTrip_supports_flat_and_root_layouts(
            final boolean useSegmentRoot) {
        final MemDirectory directory = new MemDirectory();
        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(directory);
        final SegmentId segmentId = SegmentId.of(1);
        final AsyncDirectory segmentDirectory = useSegmentRoot
                ? asyncDirectory.openSubDirectory(segmentId.getName())
                        .toCompletableFuture().join()
                : asyncDirectory;
        final SegmentConf segmentConf = new SegmentConf(8, 16, 16, 4,
                SegmentConf.UNSET_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS,
                SegmentConf.UNSET_BLOOM_FILTER_INDEX_SIZE_IN_BYTES, 0.01, 1024,
                List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        final List<Entry<Integer, String>> entries = List.of(Entry.of(1, "one"),
                Entry.of(2, "two"));

        final Segment<Integer, String> segment = applyConf(
                Segment.<Integer, String>builder(segmentDirectory)//
                        .withId(segmentId)//
                        .withKeyTypeDescriptor(keyDescriptor)//
                        .withValueTypeDescriptor(valueDescriptor),
                segmentConf).build();
        try {
            writeEntries(segment, entries);
            final long flatFileCount = directory.getFileNames().count();
            if (useSegmentRoot) {
                assertEquals(0, flatFileCount,
                        "Expected segment files under a subdirectory.");
            } else {
                assertTrue(flatFileCount > 0,
                        "Expected flat segment files in base directory.");
            }
        } finally {
            closeAndAwait(segment);
        }

        final Segment<Integer, String> reopened = applyConf(
                Segment.<Integer, String>builder(segmentDirectory)//
                        .withId(segmentId)//
                        .withKeyTypeDescriptor(keyDescriptor)//
                        .withValueTypeDescriptor(valueDescriptor),
                segmentConf).build();
        try {
            verifySegmentSearch(reopened, entries);
        } finally {
            closeAndAwait(reopened);
        }
    }

    @Test
    void compaction_updates_active_version_in_root_layout() {
        final MemDirectory directory = new MemDirectory();
        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(directory);
        final SegmentId segmentId = SegmentId.of(1);
        final AsyncDirectory segmentDirectory = asyncDirectory
                .openSubDirectory(segmentId.getName()).toCompletableFuture()
                .join();
        final SegmentConf segmentConf = new SegmentConf(8, 16, 16, 4,
                SegmentConf.UNSET_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS,
                SegmentConf.UNSET_BLOOM_FILTER_INDEX_SIZE_IN_BYTES, 0.01, 1024,
                List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        final List<Entry<Integer, String>> entries = List.of(Entry.of(1, "one"),
                Entry.of(2, "two"));

        final Segment<Integer, String> segment = applyConf(
                Segment.<Integer, String>builder(segmentDirectory)//
                        .withId(segmentId)//
                        .withKeyTypeDescriptor(keyDescriptor)//
                        .withValueTypeDescriptor(valueDescriptor),
                segmentConf).build();
        try {
            writeEntries(segment, entries);
            final SegmentResult<Void> result = segment.compact();
            assertEquals(SegmentResultStatus.OK, result.getStatus());
            awaitReady(segment);
        } finally {
            closeAndAwait(segment);
        }

        final AsyncDirectory rootDirectory = asyncDirectory
                .openSubDirectory(segmentId.getName()).toCompletableFuture()
                .join();
        final SegmentPropertiesManager propertiesManager = new SegmentPropertiesManager(
                rootDirectory, segmentId);
        assertEquals(2L, propertiesManager.getVersion());
    }

    @Test
    void root_properties_initialize_active_version() {
        final MemDirectory directory = new MemDirectory();
        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(directory);
        final SegmentId segmentId = SegmentId.of(1);
        final AsyncDirectory segmentDirectory = asyncDirectory
                .openSubDirectory(segmentId.getName()).toCompletableFuture()
                .join();
        final SegmentConf segmentConf = new SegmentConf(8, 16, 16, 4,
                SegmentConf.UNSET_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS,
                SegmentConf.UNSET_BLOOM_FILTER_INDEX_SIZE_IN_BYTES, 0.01, 1024,
                List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        final Segment<Integer, String> segment = applyConf(
                Segment.<Integer, String>builder(segmentDirectory)//
                        .withId(segmentId)//
                        .withKeyTypeDescriptor(keyDescriptor)//
                        .withValueTypeDescriptor(valueDescriptor),
                segmentConf).build();
        try {
            assertEquals(SegmentResultStatus.OK, segment.flush().getStatus());
        } finally {
            closeAndAwait(segment);
        }

        final AsyncDirectory rootDirectory = asyncDirectory
                .openSubDirectory(segmentId.getName()).toCompletableFuture()
                .join();
        final SegmentPropertiesManager propertiesManager = new SegmentPropertiesManager(
                rootDirectory, segmentId);
        assertEquals(1L, propertiesManager.getVersion());
    }

    @Test
    void root_properties_recovers_invalid_active_version() {
        final MemDirectory directory = new MemDirectory();
        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(directory);
        final SegmentId segmentId = SegmentId.of(1);
        final AsyncDirectory segmentDirectory = asyncDirectory
                .openSubDirectory(segmentId.getName()).toCompletableFuture()
                .join();
        final SegmentConf segmentConf = new SegmentConf(8, 16, 16, 4,
                SegmentConf.UNSET_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS,
                SegmentConf.UNSET_BLOOM_FILTER_INDEX_SIZE_IN_BYTES, 0.01, 1024,
                List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        final AsyncDirectory rootDirectory = asyncDirectory
                .openSubDirectory(segmentId.getName()).toCompletableFuture()
                .join();
        final SegmentPropertiesManager propertiesManager = new SegmentPropertiesManager(
                rootDirectory, segmentId);
        propertiesManager.setVersion(7L);

        final Segment<Integer, String> segment = applyConf(
                Segment.<Integer, String>builder(segmentDirectory)//
                        .withId(segmentId)//
                        .withKeyTypeDescriptor(keyDescriptor)//
                        .withValueTypeDescriptor(valueDescriptor),
                segmentConf).build();
        try {
            assertNotNull(segment);
        } finally {
            closeAndAwait(segment);
        }

        final SegmentPropertiesManager reloaded = new SegmentPropertiesManager(
                rootDirectory, segmentId);
        assertEquals(1L, reloaded.getVersion());
    }

    private SegmentBuilder<Integer, String> applyConf(
            final SegmentBuilder<Integer, String> builder,
            final SegmentConf segmentConf) {
        if (segmentConf.hasBloomFilterNumberOfHashFunctions()) {
            builder.withBloomFilterNumberOfHashFunctions(
                    segmentConf.getBloomFilterNumberOfHashFunctions());
        }
        if (segmentConf.hasBloomFilterIndexSizeInBytes()) {
            builder.withBloomFilterIndexSizeInBytes(
                    segmentConf.getBloomFilterIndexSizeInBytes());
        }
        if (segmentConf.hasBloomFilterProbabilityOfFalsePositive()) {
            builder.withBloomFilterProbabilityOfFalsePositive(
                    segmentConf.getBloomFilterProbabilityOfFalsePositive());
        }
        return builder
                .withMaxNumberOfKeysInSegmentWriteCache(
                        segmentConf.getMaxNumberOfKeysInSegmentWriteCache())
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(
                        segmentConf.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance())
                .withMaxNumberOfKeysInSegmentCache(
                        segmentConf.getMaxNumberOfKeysInSegmentCache())
                .withMaxNumberOfKeysInSegmentChunk(
                        segmentConf.getMaxNumberOfKeysInChunk())
                .withDiskIoBufferSize(segmentConf.getDiskIoBufferSize())
                .withEncodingChunkFilters(
                        segmentConf.getEncodingChunkFilters())
                .withDecodingChunkFilters(
                        segmentConf.getDecodingChunkFilters());
    }

    private static void awaitReady(final Segment<?, ?> segment) {
        final long deadline = System.nanoTime()
                + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            final SegmentState state = segment.getState();
            if (state == SegmentState.READY) {
                return;
            }
            if (state == SegmentState.ERROR) {
                throw new AssertionError("Segment failed during compact.");
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("Timed out waiting for READY state.");
    }
}
