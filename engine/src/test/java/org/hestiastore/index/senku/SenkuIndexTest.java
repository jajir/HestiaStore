package org.hestiastore.index.senku;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SenkuIndexTest {

    private MemDirectory directory;
    private TypeDescriptorInteger keyTypeDescriptor;
    private TypeDescriptorLong valueTypeDescriptor;
    private SenkuMergeFunctionRegistry<Integer, Long> functions;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        keyTypeDescriptor = new TypeDescriptorInteger();
        valueTypeDescriptor = new TypeDescriptorLong();
        functions = new SenkuMergeFunctionRegistry<>();
    }

    @Test
    void builder_returnsValidatedBuilder() {
        final SenkuIndexBuilder<Integer, Long> builder = SenkuIndex.builder(
                directory, keyTypeDescriptor, valueTypeDescriptor, functions);

        assertNotNull(builder);
        assertEquals(directory, builder.directory());
        assertEquals(keyTypeDescriptor, builder.keyTypeDescriptor());
        assertEquals(valueTypeDescriptor, builder.valueTypeDescriptor());
    }

    @Test
    void builder_rejectsNullDirectory() {
        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> SenkuIndex.builder(null, keyTypeDescriptor,
                        valueTypeDescriptor, functions));

        assertEquals("Property 'directory' must not be null.",
                error.getMessage());
    }

    @Test
    void builder_rejectsNullKeyTypeDescriptor() {
        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> SenkuIndex.builder(directory, null, valueTypeDescriptor,
                        functions));

        assertEquals("Property 'keyTypeDescriptor' must not be null.",
                error.getMessage());
    }

    @Test
    void builder_rejectsNullValueTypeDescriptor() {
        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> SenkuIndex.builder(directory, keyTypeDescriptor, null,
                        functions));

        assertEquals("Property 'valueTypeDescriptor' must not be null.",
                error.getMessage());
    }

    @Test
    void builder_rejectsNullFunctionRegistry() {
        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> SenkuIndex.builder(directory, keyTypeDescriptor,
                        valueTypeDescriptor, null));

        assertEquals("Property 'functions' must not be null.",
                error.getMessage());
    }

    @Test
    void openReturnsReadyHandleForFinalizedIndex() {
        functions.register((key, first, second) -> first + second);
        final SenkuWriting<Integer, Long> writing = SenkuIndex
                .builder(directory, keyTypeDescriptor, valueTypeDescriptor,
                        functions)
                .shardHashFunction(key -> key).shardCount(1)
                .maxInMemoryEntries(1).maxKeysPerPage(1).mergeFanIn(2)
                .maintenanceThreads(1).maintenanceQueueSize(1)
                .diskIoBufferSize(1_024).maxEntriesPerPart(1L).create();
        final SenkuReady<Integer, Long> created = writing.finishWriting();
        created.close();

        final SenkuReady<Integer, Long> opened = SenkuIndex.open(directory,
                keyTypeDescriptor, valueTypeDescriptor, 1_024);

        opened.close();
    }

    @Test
    void openRejectsNullsAndInvalidIoSize() {
        assertThrows(IllegalArgumentException.class,
                () -> SenkuIndex.open(null, keyTypeDescriptor,
                        valueTypeDescriptor, 1_024));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuIndex.open(directory, null, valueTypeDescriptor,
                        1_024));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuIndex.open(directory, keyTypeDescriptor, null,
                        1_024));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuIndex.open(directory, keyTypeDescriptor,
                        valueTypeDescriptor, 1_000));
    }

    @Test
    void wrongIoSizeFailsWhenOpeningPersistedStream() {
        functions.register((key, first, second) -> first + second);
        final SenkuWriting<Integer, Long> writing = SenkuIndex
                .builder(directory, keyTypeDescriptor, valueTypeDescriptor,
                        functions)
                .shardHashFunction(key -> key).shardCount(1)
                .maxInMemoryEntries(1).maxKeysPerPage(1).mergeFanIn(2)
                .maintenanceThreads(1).maintenanceQueueSize(1)
                .diskIoBufferSize(1_024).maxEntriesPerPart(1L).create();
        writing.put(1, 1L);
        writing.put(2, 2L);
        writing.finishWriting().close();

        final SenkuReady<Integer, Long> opened = SenkuIndex.open(directory,
                keyTypeDescriptor, valueTypeDescriptor, 2_048);
        assertThrows(IndexException.class, opened::openStream);
        opened.close();
        SenkuIndex.open(directory, keyTypeDescriptor, valueTypeDescriptor,
                1_024).close();
    }
}
