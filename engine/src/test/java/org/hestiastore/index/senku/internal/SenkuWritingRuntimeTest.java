package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.senku.SenkuReady;
import org.junit.jupiter.api.Test;

class SenkuWritingRuntimeTest {

    @Test
    void emptyFinishCreatesOneTerminalRunPerShardAndTransfersHandle() {
        final MemDirectory directory = new MemDirectory();
        final SenkuWritingRuntime<Integer, Long> writing = create(directory, 3,
                10);

        final SenkuReady<Integer, Long> ready = writing.finishWriting();

        assertEquals(SenkuWritingState.TRANSFERRED, writing.state());
        assertThrows(IndexException.class, () -> writing.put(1, 1L));
        assertThrows(IndexException.class, writing::finishWriting);
        try (Stream<Entry<Integer, Long>> stream =
                ready.openStream()) {
            assertEquals(List.of(), stream.toList());
        }
        ready.close();
    }

    @Test
    void finishFlushesUnsortedEntriesAndGloballyStreamsMergedValues() {
        final MemDirectory directory = new MemDirectory();
        final SenkuWritingRuntime<Integer, Long> writing = create(directory, 2,
                10, (key, first, second) -> first + second);
        writing.put(4, 40L);
        writing.put(1, 10L);
        writing.put(3, 30L);
        writing.put(1, 5L);
        writing.put(2, 20L);

        final SenkuReady<Integer, Long> ready = writing.finishWriting();

        try (Stream<Entry<Integer, Long>> stream =
                ready.openStream()) {
            assertEquals(List.of(Entry.of(1, 15L), Entry.of(2, 20L),
                    Entry.of(3, 30L), Entry.of(4, 40L)), stream.toList());
        }
        ready.close();
    }

    @Test
    void nullPutLeavesWritingUsableButMergeFailureMovesToError() {
        final MemDirectory directory = new MemDirectory();
        final SenkuWritingRuntime<Integer, Long> writing = create(directory, 1,
                10);
        assertThrows(IllegalArgumentException.class,
                () -> writing.put(null, 1L));
        assertEquals(SenkuWritingState.WRITING, writing.state());
        writing.put(1, 1L);

        assertThrows(IndexException.class, () -> writing.put(1, 2L));
        assertEquals(SenkuWritingState.ERROR, writing.state());
        assertThrows(IndexException.class, writing::finishWriting);
    }

    @Test
    void exactThresholdFlushesParticipateInFinalDrain() {
        final MemDirectory directory = new MemDirectory();
        final SenkuWritingRuntime<Integer, Long> writing = create(directory, 2,
                1, (key, first, second) -> first + second);
        writing.put(3, 30L);
        writing.put(1, 10L);

        final SenkuReady<Integer, Long> ready = writing.finishWriting();

        try (Stream<Entry<Integer, Long>> stream =
                ready.openStream()) {
            assertEquals(List.of(Entry.of(1, 10L), Entry.of(3, 30L)),
                    stream.toList());
        }
        ready.close();
        assertFalse(directory.isFileExists(SenkuFileNames.LOCK_FILE));
    }

    private static SenkuWritingRuntime<Integer, Long> create(
            final MemDirectory directory, final int shardCount,
            final int maxInMemoryEntries) {
        return create(directory, shardCount, maxInMemoryEntries,
                (key, first, second) -> {
                    throw new IllegalStateException("merge failure");
                });
    }

    @SuppressWarnings("unchecked")
    private static SenkuWritingRuntime<Integer, Long> create(
            final MemDirectory directory, final int shardCount,
            final int maxInMemoryEntries,
            final org.hestiastore.index.senku.SenkuMergeFunction<Integer, Long> merge) {
        return (SenkuWritingRuntime<Integer, Long>) SenkuRuntime.create(directory,
                new TypeDescriptorInteger(), new TypeDescriptorLong(), merge,
                key -> key, shardCount, maxInMemoryEntries,
                maxInMemoryEntries * 2, 1, 2, 2, 2, 1_024, 2L);
    }
}
