package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class SegmentIndexImplPutTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    @Test
    void putWritesDirectlyToSegment() {
        final IndexInternalDefault<Integer, String> index = new IndexInternalDefault<>(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(new MemDirectory()),
                tdi, tds, buildConf(10, 1));

        index.put(1, "one");

        assertEquals("one", index.get(1));
        index.close();
    }

    @Test
    void putRejectsTombstoneValues() {
        final IndexInternalDefault<Integer, String> index = new IndexInternalDefault<>(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(new MemDirectory()),
                tdi, tds, buildConf(10, 2));

        assertThrows(IllegalArgumentException.class,
                () -> index.put(1, TypeDescriptorShortString.TOMBSTONE_VALUE));
        index.close();
    }

    @Test
    void putOptionallySplitsSegmentWhenThresholdReached() {
        final IndexInternalDefault<Integer, String> index = new IndexInternalDefault<>(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(new MemDirectory()),
                tdi, tds, buildConf(4, 1));

        index.put(1, "a");
        index.put(2, "b");
        index.put(3, "c");
        index.put(4, "d");
        index.put(5, "e");

        final KeySegmentCache<Integer> cache = readKeySegmentCache(index);
        awaitSegmentCount(cache, 2);
        assertEquals(SegmentId.of(1), cache.findSegmentId(1));
        index.close();
    }

    @Test
    void deleteWritesTombstoneToSegment() {
        final IndexInternalDefault<Integer, String> index = new IndexInternalDefault<>(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(new MemDirectory()),
                tdi, tds, buildConf(10, 1));

        index.put(1, "one");
        index.delete(1);

        assertNull(index.get(1));
        index.close();
    }

    private IndexConfiguration<Integer, String> buildConf(
            final int maxKeysInSegment,
            final int maxNumberOfKeysInSegmentWriteCache) {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withName("test-index")//
                .withMaxNumberOfKeysInSegmentCache(4)//
                .withMaxNumberOfKeysInSegmentWriteCache(
                        maxNumberOfKeysInSegmentWriteCache)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInCache(10)//
                .withMaxNumberOfKeysInSegment(maxKeysInSegment)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withContextLoggingEnabled(false)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }

    private static void awaitSegmentCount(final KeySegmentCache<Integer> cache,
            final int expectedCount) {
        final long deadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            if (cache.getSegmentIds().size() == expectedCount) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        assertEquals(expectedCount, cache.getSegmentIds().size());
    }

    @SuppressWarnings("unchecked")
    private static <K, V> KeySegmentCache<K> readKeySegmentCache(
            final SegmentIndexImpl<K, V> index) {
        try {
            final Field field = SegmentIndexImpl.class
                    .getDeclaredField("keySegmentCache");
            field.setAccessible(true);
            return (KeySegmentCache<K>) field.get(index);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read keySegmentCache for test", ex);
        }
    }
}
