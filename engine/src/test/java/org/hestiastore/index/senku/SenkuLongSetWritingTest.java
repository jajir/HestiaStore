package org.hestiastore.index.senku;

import static org.hestiastore.index.datatype.NullValue.NULL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.chunkentryfile.KeyPageCodecs;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SenkuLongSetWritingTest {

    @Test
    void synchronousSlicesSupportCallerReuseDuplicatesExtremesAndInvalidRanges() {
        final MemDirectory directory = new MemDirectory();
        final AtomicInteger hashCalls = new AtomicInteger();
        final SenkuLongSetWriting writing = builder(directory)
                .maxInMemoryEntries(700).createLongSet(key -> {
                    hashCalls.incrementAndGet();
                    return Long.hashCode(key);
                });
        final long[] input = new long[6005];
        for (int index = 0; index < 6000; index++)
            input[index + 3] = index % 600;
        assertThrows(IllegalArgumentException.class,
                () -> writing.putLongs(null, 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> writing.putLongs(input, -1, 1));
        assertThrows(IllegalArgumentException.class,
                () -> writing.putLongs(input, 0, -1));
        assertThrows(IllegalArgumentException.class,
                () -> writing.putLongs(input, 3, input.length));
        assertThrows(IllegalArgumentException.class, () -> writing
                .putLongs(input, Integer.MAX_VALUE, Integer.MAX_VALUE));
        writing.putLongs(input, input.length, 0);
        assertEquals(0, hashCalls.get());
        writing.putLongs(input, 3, 6000);
        Arrays.fill(input, -999);
        final long[] extremes = { Long.MIN_VALUE, 0, Long.MAX_VALUE, 0 };
        writing.putLongs(extremes, 0, extremes.length);
        Arrays.fill(extremes, -999);
        final List<Long> expected = LongStream
                .concat(LongStream.of(Long.MIN_VALUE),
                        LongStream.concat(LongStream.range(0, 600),
                                LongStream.of(Long.MAX_VALUE)))
                .boxed().toList();
        try (SenkuReady<Long, NullValue> ready = writing.finishWriting();
                Stream<Entry<Long, NullValue>> entries = ready.openStream()) {
            assertEquals(expected, entries.map(Entry::getKey).toList());
            assertEquals(6004, hashCalls.get(),
                    "Flush must reuse configured hashes");
            assertThrows(IndexException.class,
                    () -> writing.putLongs(input, 0, 0));
            assertThrows(IndexException.class,
                    () -> writing.putLongs(input, 0, 1));
        }
        try (SenkuReady<Long, NullValue> ready = SenkuIndex.open(directory,
                new TypeDescriptorLong(), new TypeDescriptorNull(), 8192);
                Stream<Entry<Long, NullValue>> entries = ready.openStream()) {
            assertEquals(expected, entries.map(Entry::getKey).toList());
        }
    }

    @Test
    void operationalBatchHashFailurePoisonsWriterUnlikeArgumentErrors() {
        final MemDirectory directory = new MemDirectory();
        final IllegalStateException cause = new IllegalStateException(
                "batch routing");
        final SenkuLongSetWriting writing = builder(directory)
                .createLongSet(key -> {
                    throw cause;
                });
        final long[] input = { 0 };
        final IndexException failure = assertThrows(IndexException.class,
                () -> writing.putLongs(input, 0, 1));
        assertEquals(cause, failure.getCause());
        assertThrows(IndexException.class, () -> writing.putLongs(input, 0, 0));
        assertThrows(IndexException.class, writing::finishWriting);
    }

    @Test
    void primitiveAndCompatibilityPutsDeduplicateAcrossFlushesAndReopen() {
        final MemDirectory directory = new MemDirectory();
        final SenkuLongSetWriting writing = builder(directory)
                .keyPageCodec(KeyPageCodecs.longDeltaVarint())
                .shardHashFunction(key -> {
                    throw new AssertionError("Generic hash must not be used");
                }).createLongSet(key -> 0);
        final List<Long> expected = List.of(Long.MIN_VALUE, -1L, 0L, 1L,
                Long.MAX_VALUE);
        for (int pass = 0; pass < 3; pass++) {
            for (final long key : expected) {
                writing.putLong(key);
                writing.put(key, NULL);
            }
        }
        assertThrows(IllegalArgumentException.class,
                () -> writing.put(null, NULL));
        assertThrows(IllegalArgumentException.class,
                () -> writing.put(0L, null));
        assertThrows(IllegalArgumentException.class,
                () -> writing.put(0L, NullValue.TOMBSTONE));
        try (SenkuReady<Long, NullValue> ready = writing.finishWriting();
                Stream<Entry<Long, NullValue>> entries = ready.openStream()) {
            assertEquals(expected, entries.map(Entry::getKey).toList());
            assertThrows(IndexException.class, () -> writing.putLong(2L));
            assertThrows(IndexException.class, writing::finishWriting);
        }
        try (SenkuReady<Long, NullValue> ready = SenkuIndex.open(directory,
                new TypeDescriptorLong(), new TypeDescriptorNull(), 8192);
                Stream<Entry<Long, NullValue>> entries = ready.openStream()) {
            assertEquals(expected, entries.map(Entry::getKey).toList());
        }
    }

    @Test
    void failedPrimitiveHashPreservesFirstFailureAndReleasesLifecycle() {
        final MemDirectory directory = new MemDirectory();
        final IllegalStateException cause = new IllegalStateException(
                "bad hash");
        final SenkuLongSetWriting writing = builder(directory)
                .createLongSet(key -> {
                    throw cause;
                });
        final IndexException failure = assertThrows(IndexException.class,
                () -> writing.putLong(0L));
        assertEquals(cause, failure.getCause());
        assertThrows(IndexException.class, writing::finishWriting);
        assertThrows(IndexException.class, () -> writing.putLong(0L));
    }

    @Test
    void selectingSetRequiresExplicitReducerAndExactDescriptorsBeforeStorage() {
        final MemDirectory directory = new MemDirectory();
        final SenkuMergeFunctionRegistry<Long, NullValue> generic = new SenkuMergeFunctionRegistry<>();
        generic.register((key, first, second) -> NULL);
        final SenkuIndexBuilder<Long, NullValue> genericBuilder = SenkuIndex
                .builder(directory, new TypeDescriptorLong(),
                        new TypeDescriptorNull(), generic);
        assertThrows(IllegalArgumentException.class,
                () -> genericBuilder.createLongSet(Long::hashCode));
        assertThrows(IllegalArgumentException.class,
                () -> builder(directory).createLongSet(null));
        final SenkuMergeFunctionRegistry<Long, NullValue> set = new SenkuMergeFunctionRegistry<>();
        set.register(SenkuMergeFunctions.longSet());
        final SenkuIndexBuilder<Long, NullValue> subclass = SenkuIndex
                .builder(directory, new TypeDescriptorLong() {
                }, new TypeDescriptorNull(), set);
        assertThrows(IllegalArgumentException.class,
                () -> subclass.createLongSet(Long::hashCode));
        assertEquals(0L, directory.getFileNames().count());
    }

    private static SenkuIndexBuilder<Long, NullValue> builder(
            final MemDirectory directory) {
        final SenkuMergeFunctionRegistry<Long, NullValue> functions = new SenkuMergeFunctionRegistry<>();
        functions.register(SenkuMergeFunctions.longSet());
        return SenkuIndex
                .builder(directory, new TypeDescriptorLong(),
                        new TypeDescriptorNull(), functions)
                .shardCount(2).maxInMemoryEntries(3).maxKeysPerPage(2)
                .maxEntriesPerPart(1000).mergeFanIn(3).maintenanceThreads(2)
                .maintenanceQueueSize(8).diskIoBufferSize(8192);
    }
}
