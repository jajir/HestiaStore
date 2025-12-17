package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IntegrationSegmentIndexAsyncTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void putAsyncAndGetAsyncRoundTrip(final boolean threadSafe) {
        final Directory directory = new MemDirectory();
        final SegmentIndex<Integer, String> index = newIndex(directory,
                threadSafe);
        try {
            final List<CompletableFuture<Void>> writes = IntStream.range(0, 20)
                    .mapToObj(i -> index.putAsync(i, "value-" + i)
                            .toCompletableFuture())
                    .toList();
            CompletableFuture
                    .allOf(writes.toArray(new CompletableFuture[0])).join();

            final List<CompletableFuture<String>> reads = IntStream
                    .range(0, 20)
                    .mapToObj(i -> index.getAsync(i).toCompletableFuture())
                    .toList();
            CompletableFuture
                    .allOf(reads.toArray(new CompletableFuture[0])).join();

            IntStream.range(0, 20).forEach(i -> {
                assertEquals("value-" + i, reads.get(i).join());
            });
        } finally {
            index.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void deleteAsyncRemovesKeysAndPersistsAcrossReopen(
            final boolean threadSafe) {
        final Directory directory = new MemDirectory();
        final SegmentIndex<Integer, String> index = newIndex(directory,
                threadSafe);
        try {
            IntStream.range(0, 6).forEach(i -> index.put(i, "value-" + i));

            final CompletableFuture<Void> deleteTwo = index.deleteAsync(2)
                    .toCompletableFuture();
            final CompletableFuture<Void> deleteFour = index.deleteAsync(4)
                    .toCompletableFuture();
            CompletableFuture.allOf(deleteTwo, deleteFour).join();

            assertNull(index.getAsync(2).toCompletableFuture().join());
            assertNull(index.get(4));
            assertEquals("value-3",
                    index.getAsync(3).toCompletableFuture().join());
        } finally {
            index.close();
        }

        final SegmentIndex<Integer, String> reopenedIndex = SegmentIndex
                .open(directory);
        try {
            assertNull(reopenedIndex.get(2));
            assertNull(reopenedIndex.get(4));
            assertEquals("value-0", reopenedIndex.get(0));
            assertEquals("value-5", reopenedIndex.get(5));
        } finally {
            reopenedIndex.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true })
    void asyncOperationsInterleaveWithSyncOperations(
            final boolean threadSafe) {
        final Directory directory = new MemDirectory();
        final SegmentIndex<Integer, String> index = newIndex(directory,
                threadSafe);
        try {
            final CompletableFuture<Void> asyncPut = index
                    .putAsync(100, "async-100").toCompletableFuture();
            index.put(200, "sync-200");

            asyncPut.join();

            assertEquals("async-100",
                    index.getAsync(100).toCompletableFuture().join());
            assertEquals("sync-200",
                    index.getAsync(200).toCompletableFuture().join());

            index.flush();
            assertEquals(2,
                    index.getStream(SegmentWindow.unbounded()).count());
        } finally {
            index.close();
        }
    }

    @Test
    void getAsyncReturnsNullForMissingKey() {
        final Directory directory = new MemDirectory();
        final SegmentIndex<Integer, String> index = newIndex(directory, false);
        try {
            assertNull(index.getAsync(999).toCompletableFuture().join());
        } finally {
            index.close();
        }
    }

    private SegmentIndex<Integer, String> newIndex(final Directory directory,
            final boolean threadSafe) {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi) //
                .withValueTypeDescriptor(tds) //
                .withMaxNumberOfKeysInSegment(4) //
                .withMaxNumberOfKeysInSegmentCache(3) //
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(4) //
                .withMaxNumberOfKeysInSegmentChunk(2) //
                .withMaxNumberOfKeysInCache(3) //
                .withMaxNumberOfSegmentsInCache(3) //
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(3) //
                .withContextLoggingEnabled(true) //
                .withThreadSafe(threadSafe)//
                .withName("async_index") //
                .build();
        return SegmentIndex.create(directory, conf);
    }
}
