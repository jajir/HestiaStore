package org.hestiastore.index.senku;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SenkuReadyTest {

    @Test
    void contractIsCloseableAndExposesMetadataAndStreamLifecycle() {
        final Set<String> methodNames = Arrays
                .stream(SenkuReady.class.getDeclaredMethods())
                .map(Method::getName).collect(Collectors.toSet());
        assertEquals(
                Set.of("openStream", "close", "recordCount", "longKeySummary"),
                methodNames);
        assertTrue(AutoCloseable.class.isAssignableFrom(SenkuReady.class));
    }

    @Test
    void publicReadyCountAndSummarySurviveReopenAndRejectClosedAccess() {
        final MemDirectory directory = new MemDirectory();
        final TypeDescriptorLong longs = new TypeDescriptorLong();
        final SenkuMergeFunctionRegistry<Long, Long> merges = new SenkuMergeFunctionRegistry<>();
        merges.register((key, left, right) -> left + right);
        final SenkuWriting<Long, Long> writing = SenkuIndex
                .builder(directory, longs, longs, merges)
                .shardHashFunction(key -> Long.hashCode(key)).shardCount(1)
                .maxInMemoryEntries(4).maxKeysPerPage(2).maxEntriesPerPart(4)
                .mergeFanIn(2).maintenanceThreads(1).maintenanceQueueSize(2)
                .diskIoBufferSize(1024).create();
        writing.put(3L, 1L);
        writing.put(3L, 2L);
        writing.put(7L, 3L);
        final SenkuReady<Long, Long> ready = writing.finishWriting();
        assertEquals(2, ready.recordCount());
        assertEquals(2, ready.longKeySummary().orElseThrow().recordCount());
        ready.close();
        assertThrows(IndexException.class, ready::recordCount);
        try (var reopened = SenkuIndex.open(directory, longs, longs, 1024);
                var stream = reopened.openStream()) {
            assertEquals(2, reopened.recordCount());
            assertEquals(2, stream.count());
            assertEquals(2,
                    reopened.longKeySummary().orElseThrow().recordCount());
        }
    }
}
