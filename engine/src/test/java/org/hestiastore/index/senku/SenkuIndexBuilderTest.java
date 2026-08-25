package org.hestiastore.index.senku;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfigurationDefaults;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SenkuIndexBuilderTest {

    private MemDirectory directory;
    private TypeDescriptorInteger keyTypeDescriptor;
    private TypeDescriptorLong valueTypeDescriptor;
    private SenkuMergeFunctionRegistry<Integer, Long> functions;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        keyTypeDescriptor = new TypeDescriptorInteger();
        valueTypeDescriptor = new TypeDescriptorLong();
        functions = registeredFunctions();
    }

    @Test
    void builder_usesDocumentedDefaults() {
        final SenkuIndexBuilder<Integer, Long> builder = newBuilder();

        assertEquals(1_000_000, builder.maxKeysPerPage());
        assertEquals(10_000_000L, builder.maxEntriesPerPart());
        assertEquals(42, builder.maintenanceQueueSize());
        assertEquals(
                IndexConfigurationDefaults.DEFAULT_DISK_IO_BUFFER_SIZE_BYTES,
                builder.diskIoBufferSize());
    }

    @Test
    void setters_areFluentAndUseLastValue() {
        final SenkuIndexBuilder<Integer, Long> builder = newBuilder();

        assertSame(builder,
                builder.shardHashFunction(value -> value.hashCode()));
        assertSame(builder, builder.shardCount(4).shardCount(8));
        assertSame(builder,
                builder.maxInMemoryEntries(100).maxInMemoryEntries(200));
        assertSame(builder, builder.maxKeysPerPage(10));
        assertSame(builder, builder.mergeFanIn(3));
        assertSame(builder, builder.maintenanceThreads(2));
        assertSame(builder, builder.maintenanceQueueSize(5));
        assertSame(builder, builder.diskIoBufferSize(2_048));
        assertSame(builder, builder.maxEntriesPerPart(20L));

        builder.validate();
        assertEquals(8, builder.shardCount());
        assertEquals(200, builder.maxInMemoryEntries());
        assertEquals(10, builder.maxKeysPerPage());
        assertEquals(3, builder.mergeFanIn());
        assertEquals(2, builder.maintenanceThreads());
        assertEquals(5, builder.maintenanceQueueSize());
        assertEquals(2_048, builder.diskIoBufferSize());
        assertEquals(20L, builder.maxEntriesPerPart());
    }

    @Test
    void validate_acceptsAllDocumentedUpperBoundaries() {
        final SenkuIndexBuilder<Integer, Long> builder = newBuilder()
                .shardHashFunction(value -> value.hashCode())
                .shardCount(SenkuIndexBuilder.MAX_SHARD_COUNT)
                .maxInMemoryEntries(
                        SenkuIndexBuilder.MAX_IN_MEMORY_ENTRIES)
                .mergeFanIn(Integer.MAX_VALUE)
                .maintenanceThreads(Integer.MAX_VALUE);

        builder.validate();

        assertEquals(1 << 30, builder.initialMapCapacity());
        assertEquals(20_000_000, builder.shardIndexBytes());
    }

    @Test
    void validate_rejectsMissingHashFunction() {
        final SenkuIndexBuilder<Integer, Long> builder = newBuilder()
                .shardCount(1).maxInMemoryEntries(1).mergeFanIn(2)
                .maintenanceThreads(1);

        assertMissingSetting(builder, "shardHashFunction");
    }

    @Test
    void validate_rejectsMissingShardCount() {
        final SenkuIndexBuilder<Integer, Long> builder = newBuilder()
                .shardHashFunction(value -> value.hashCode())
                .maxInMemoryEntries(1)
                .mergeFanIn(2).maintenanceThreads(1);

        assertMissingSetting(builder, "shardCount");
    }

    @Test
    void validate_rejectsMissingMaxInMemoryEntries() {
        final SenkuIndexBuilder<Integer, Long> builder = newBuilder()
                .shardHashFunction(value -> value.hashCode()).shardCount(1)
                .mergeFanIn(2).maintenanceThreads(1);

        assertMissingSetting(builder, "maxInMemoryEntries");
    }

    @Test
    void validate_rejectsMissingMergeFanIn() {
        final SenkuIndexBuilder<Integer, Long> builder = newBuilder()
                .shardHashFunction(value -> value.hashCode()).shardCount(1)
                .maxInMemoryEntries(1).maintenanceThreads(1);

        assertMissingSetting(builder, "mergeFanIn");
    }

    @Test
    void validate_rejectsMissingMaintenanceThreads() {
        final SenkuIndexBuilder<Integer, Long> builder = newBuilder()
                .shardHashFunction(value -> value.hashCode()).shardCount(1)
                .maxInMemoryEntries(1).mergeFanIn(2);

        assertMissingSetting(builder, "maintenanceThreads");
    }

    @Test
    void validate_rejectsEmptyMergeRegistry() {
        functions = new SenkuMergeFunctionRegistry<>();
        final SenkuIndexBuilder<Integer, Long> builder = validBuilder();

        final IndexException error = assertThrows(IndexException.class,
                builder::validate);

        assertEquals(
                "Senku merge function registry requires exactly one function.",
                error.getMessage());
    }

    @Test
    void validate_rejectsPartLimitBelowPageLimit() {
        final SenkuIndexBuilder<Integer, Long> builder = validBuilder()
                .maxKeysPerPage(11).maxEntriesPerPart(10L);

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, builder::validate);

        assertEquals(
                "Property 'maxEntriesPerPart' must be greater than or equal to maxKeysPerPage",
                error.getMessage());
    }

    @Test
    void shardHashFunction_rejectsNull() {
        final SenkuIndexBuilder<Integer, Long> builder = newBuilder();

        assertInvalidSetting(() -> builder.shardHashFunction(null),
                "shardHashFunction");
    }

    @Test
    void shardCount_rejectsOutOfRangeValues() {
        final SenkuIndexBuilder<Integer, Long> builder = newBuilder();

        assertInvalidSetting(() -> builder.shardCount(0), "shardCount");
        assertInvalidSetting(
                () -> builder.shardCount(
                        SenkuIndexBuilder.MAX_SHARD_COUNT + 1),
                "shardCount");
    }

    @Test
    void maxInMemoryEntries_rejectsOutOfRangeValues() {
        final SenkuIndexBuilder<Integer, Long> builder = newBuilder();

        assertInvalidSetting(() -> builder.maxInMemoryEntries(0),
                "maxInMemoryEntries");
        assertInvalidSetting(
                () -> builder.maxInMemoryEntries(
                        SenkuIndexBuilder.MAX_IN_MEMORY_ENTRIES + 1),
                "maxInMemoryEntries");
    }

    @Test
    void positiveSettings_rejectNonPositiveValues() {
        final SenkuIndexBuilder<Integer, Long> builder = newBuilder();

        assertInvalidSetting(() -> builder.maxKeysPerPage(0),
                "maxKeysPerPage");
        assertInvalidSetting(() -> builder.maintenanceThreads(0),
                "maintenanceThreads");
        assertInvalidSetting(() -> builder.maintenanceQueueSize(0),
                "maintenanceQueueSize");
        assertInvalidSetting(() -> builder.maxEntriesPerPart(0L),
                "maxEntriesPerPart");
    }

    @Test
    void mergeFanIn_rejectsValuesBelowTwo() {
        final SenkuIndexBuilder<Integer, Long> builder = newBuilder();

        assertInvalidSetting(() -> builder.mergeFanIn(1), "mergeFanIn");
    }

    @Test
    void diskIoBufferSize_rejectsInvalidValueAndNamesSetting() {
        final SenkuIndexBuilder<Integer, Long> builder = newBuilder();

        assertInvalidSetting(() -> builder.diskIoBufferSize(1_000),
                "diskIoBufferSize");
    }

    @Test
    void initialMapCapacity_usesOverflowSafeSizing() {
        final SenkuIndexBuilder<Integer, Long> builder = newBuilder()
                .maxInMemoryEntries(1);

        assertEquals(2, builder.initialMapCapacity());
    }

    @Test
    void createReturnsWorkingHandleAndFreezesFunctionRegistry() {
        final SenkuIndexBuilder<Integer, Long> builder = validBuilder()
                .shardCount(1).maxInMemoryEntries(2).maxKeysPerPage(1)
                .mergeFanIn(2).maintenanceThreads(1)
                .maintenanceQueueSize(1).diskIoBufferSize(1_024)
                .maxEntriesPerPart(2L);

        final SenkuWriting<Integer, Long> writing = builder.create();

        assertThrows(IndexException.class,
                () -> functions.register((key, first, second) -> first));
        writing.finishWriting().close();
    }

    private SenkuIndexBuilder<Integer, Long> validBuilder() {
        return newBuilder().shardHashFunction(value -> value.hashCode())
                .shardCount(4).maxInMemoryEntries(100).mergeFanIn(8)
                .maintenanceThreads(2);
    }

    private SenkuIndexBuilder<Integer, Long> newBuilder() {
        return SenkuIndex.builder(directory, keyTypeDescriptor,
                valueTypeDescriptor, functions);
    }

    private static SenkuMergeFunctionRegistry<Integer, Long> registeredFunctions() {
        return new SenkuMergeFunctionRegistry<Integer, Long>()
                .register((key, first, second) -> first + second);
    }

    private static void assertMissingSetting(
            final SenkuIndexBuilder<Integer, Long> builder,
            final String setting) {
        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, builder::validate);
        assertEquals("Property '" + setting + "' must not be null.",
                error.getMessage());
    }

    private static void assertInvalidSetting(final Runnable operation,
            final String setting) {
        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, operation::run);
        assertTrue(error.getMessage().contains(setting));
    }
}
