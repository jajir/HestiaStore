package org.hestiastore.index.segment;

import static org.hestiastore.index.segment.SegmentTestHelper.closeAndAssertClosed;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
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
        final Directory asyncDirectory = directory;
        final SegmentId segmentId = SegmentId.of(1);
        final Directory segmentDirectory = useSegmentRoot
                ? asyncDirectory.openSubDirectory(segmentId.getName())
                : asyncDirectory;
        final SegmentConf segmentConf = createSegmentConf();
        final List<Entry<Integer, String>> entries = List.of(Entry.of(1, "one"),
                Entry.of(2, "two"));

        final Segment<Integer, String> segment = applyConf(
                Segment.<Integer, String>builder(segmentDirectory)//
                        .withId(segmentId)//
                        .withKeyTypeDescriptor(keyDescriptor)//
                        .withValueTypeDescriptor(valueDescriptor),
                segmentConf).build().getValue();
        try {
            writeEntries(segment, entries);
            final long flatFileCount = directory.getFileNames()
                    .filter(name -> name.contains(".")).count();
            if (useSegmentRoot) {
                assertEquals(0, flatFileCount,
                        "Expected segment files under a subdirectory.");
            } else {
                assertTrue(flatFileCount > 0,
                        "Expected flat segment files in base directory.");
            }
        } finally {
            closeAndAssertClosed(segment);
        }

        final Segment<Integer, String> reopened = applyConf(
                Segment.<Integer, String>builder(segmentDirectory)//
                        .withId(segmentId)//
                        .withKeyTypeDescriptor(keyDescriptor)//
                        .withValueTypeDescriptor(valueDescriptor),
                segmentConf).build().getValue();
        try {
            verifySegmentSearch(reopened, entries);
        } finally {
            closeAndAssertClosed(reopened);
        }
    }

    @Test
    void compaction_updates_active_version_in_root_layout() {
        final MemDirectory directory = new MemDirectory();
        final Directory asyncDirectory = directory;
        final SegmentId segmentId = SegmentId.of(1);
        final Directory segmentDirectory = asyncDirectory
                .openSubDirectory(segmentId.getName());
        final SegmentConf segmentConf = createSegmentConf();
        final List<Entry<Integer, String>> entries = List.of(Entry.of(1, "one"),
                Entry.of(2, "two"));

        final Segment<Integer, String> segment = applyConf(
                Segment.<Integer, String>builder(segmentDirectory)//
                        .withId(segmentId)//
                        .withKeyTypeDescriptor(keyDescriptor)//
                        .withValueTypeDescriptor(valueDescriptor),
                segmentConf).build().getValue();
        try {
            writeEntries(segment, entries);
            final SegmentResult<Void> result = segment.compact();
            assertEquals(SegmentResultStatus.OK, result.getStatus());
            awaitReady(segment);
        } finally {
            closeAndAssertClosed(segment);
        }

        final Directory rootDirectory = asyncDirectory
                .openSubDirectory(segmentId.getName());
        final SegmentPropertiesManager propertiesManager = new SegmentPropertiesManager(
                rootDirectory, segmentId);
        assertEquals(2L, propertiesManager.getVersion());
    }

    @Test
    void root_properties_initialize_active_version() {
        final MemDirectory directory = new MemDirectory();
        final Directory asyncDirectory = directory;
        final SegmentId segmentId = SegmentId.of(1);
        final Directory segmentDirectory = asyncDirectory
                .openSubDirectory(segmentId.getName());
        final SegmentConf segmentConf = createSegmentConf();
        final Segment<Integer, String> segment = applyConf(
                Segment.<Integer, String>builder(segmentDirectory)//
                        .withId(segmentId)//
                        .withKeyTypeDescriptor(keyDescriptor)//
                        .withValueTypeDescriptor(valueDescriptor),
                segmentConf).build().getValue();
        try {
            assertEquals(SegmentResultStatus.OK, segment.flush().getStatus());
        } finally {
            closeAndAssertClosed(segment);
        }

        final Directory rootDirectory = asyncDirectory
                .openSubDirectory(segmentId.getName());
        final SegmentPropertiesManager propertiesManager = new SegmentPropertiesManager(
                rootDirectory, segmentId);
        assertEquals(1L, propertiesManager.getVersion());
    }

    @Test
    void root_properties_recovers_invalid_active_version() {
        final MemDirectory directory = new MemDirectory();
        final Directory asyncDirectory = directory;
        final SegmentId segmentId = SegmentId.of(1);
        final Directory segmentDirectory = asyncDirectory
                .openSubDirectory(segmentId.getName());
        final SegmentConf segmentConf = createSegmentConf();
        final Directory rootDirectory = asyncDirectory
                .openSubDirectory(segmentId.getName());
        final SegmentPropertiesManager propertiesManager = new SegmentPropertiesManager(
                rootDirectory, segmentId);
        propertiesManager.startTx().setVersion(7L).commit();

        final Segment<Integer, String> segment = applyConf(
                Segment.<Integer, String>builder(segmentDirectory)//
                        .withId(segmentId)//
                        .withKeyTypeDescriptor(keyDescriptor)//
                        .withValueTypeDescriptor(valueDescriptor),
                segmentConf).build().getValue();
        try {
            assertNotNull(segment);
        } finally {
            closeAndAssertClosed(segment);
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
                .withMaintenancePolicy(new SegmentMaintenancePolicyThreshold<>(
                        segmentConf.getMaxNumberOfKeysInSegmentCache(),
                        segmentConf.getMaxNumberOfKeysInSegmentWriteCache(),
                        segmentConf.getMaxNumberOfDeltaCacheFiles()))
                .withDiskIoBufferSize(segmentConf.getDiskIoBufferSize())
                .withEncodingChunkFilters(
                        segmentConf.getEncodingChunkFilters())
                .withDecodingChunkFilters(
                        segmentConf.getDecodingChunkFilters());
    }

    private static SegmentConf createSegmentConf() {
        return SegmentConf.builder()
                .withMaxNumberOfKeysInSegmentWriteCache(8)
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(16)
                .withMaxNumberOfKeysInSegmentCache(16)
                .withMaxNumberOfKeysInChunk(4)
                .withMaxNumberOfDeltaCacheFiles(5)
                .withBloomFilterNumberOfHashFunctions(
                        SegmentConf.UNSET_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS)
                .withBloomFilterIndexSizeInBytes(
                        SegmentConf.UNSET_BLOOM_FILTER_INDEX_SIZE_IN_BYTES)
                .withBloomFilterProbabilityOfFalsePositive(0.01)
                .withDiskIoBufferSize(1024)
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .build();
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
