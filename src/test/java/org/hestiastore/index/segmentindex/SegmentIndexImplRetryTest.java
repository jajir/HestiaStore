package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexImplRetryTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    @Mock
    private Segment<Integer, String> segment;

    @Test
    void getRetriesOnBusyThenOk() {
        final IndexInternalDefault<Integer, String> index = newIndex();
        try {
            index.put(1, "one");

            final KeyToSegmentMapSynchronizedAdapter<Integer> cache = readKeyToSegmentMap(
                    index);
            final SegmentId segmentId = cache.findSegmentId(1);
            final SegmentRegistry<Integer, String> registry = readSegmentRegistry(
                    index);
            final Segment<Integer, String> original = registry
                    .getSegment(segmentId).getValue();

            final AtomicInteger attempts = new AtomicInteger();
            when(segment.get(eq(1))).thenAnswer(invocation -> {
                if (attempts.getAndIncrement() == 0) {
                    return SegmentResult.busy();
                }
                return SegmentResult.ok("value");
            });

            replaceSegment(registry, segmentId, segment);

            assertEquals("value", index.get(1));
            verify(segment, times(2)).get(1);

            replaceSegment(registry, segmentId, original);
        } finally {
            index.close();
        }
    }

    @Test
    void putRetriesOnBusyThenOk() {
        final IndexInternalDefault<Integer, String> index = newIndex();
        try {
            index.put(1, "one");

            final KeyToSegmentMapSynchronizedAdapter<Integer> cache = readKeyToSegmentMap(
                    index);
            final SegmentId segmentId = cache.findSegmentId(1);
            final SegmentRegistry<Integer, String> registry = readSegmentRegistry(
                    index);
            final Segment<Integer, String> original = registry
                    .getSegment(segmentId).getValue();

            final AtomicInteger attempts = new AtomicInteger();
            when(segment.put(eq(2), eq("two"))).thenAnswer(invocation -> {
                if (attempts.getAndIncrement() == 0) {
                    return SegmentResult.busy();
                }
                return SegmentResult.ok();
            });

            replaceSegment(registry, segmentId, segment);

            index.put(2, "two");

            verify(segment, times(2)).put(2, "two");

            replaceSegment(registry, segmentId, original);
        } finally {
            index.close();
        }
    }

    private IndexInternalDefault<Integer, String> newIndex() {
        return new IndexInternalDefault<>(
                AsyncDirectoryAdapter.wrap(new MemDirectory()), tdi, tds,
                buildConf());
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class).withValueClass(String.class)
                .withKeyTypeDescriptor(tdi).withValueTypeDescriptor(tds)
                .withName("segment-index-retry-test")
                .withContextLoggingEnabled(false)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInSegmentWriteCache(5)
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(6)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withMaxNumberOfKeysInCache(10)
                .withMaxNumberOfKeysInSegment(100)
                .withMaxNumberOfSegmentsInCache(3)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSizeInBytes(1024).withNumberOfCpuThreads(1)
                .withNumberOfIoThreads(1)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }

    private static <K, V> void replaceSegment(
            final SegmentRegistry<K, V> registry, final SegmentId segmentId,
            final Segment<K, V> segment) {
        final Map<SegmentId, Segment<K, V>> segments = readSegmentsMap(
                registry);
        registry.executeWithRegistryLock(
                () -> segments.put(segmentId, segment));
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentRegistry<K, V> readSegmentRegistry(
            final SegmentIndexImpl<K, V> index) {
        try {
            final Field field = SegmentIndexImpl.class
                    .getDeclaredField("segmentRegistry");
            field.setAccessible(true);
            return (SegmentRegistry<K, V>) field.get(index);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read segmentRegistry for test", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private static <K, V> KeyToSegmentMapSynchronizedAdapter<K> readKeyToSegmentMap(
            final SegmentIndexImpl<K, V> index) {
        try {
            final Field field = SegmentIndexImpl.class
                    .getDeclaredField("keyToSegmentMap");
            field.setAccessible(true);
            return (KeyToSegmentMapSynchronizedAdapter<K>) field.get(index);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read keyToSegmentMap for test", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private static <K, V> Map<SegmentId, Segment<K, V>> readSegmentsMap(
            final SegmentRegistry<K, V> registry) {
        try {
            final Field field = SegmentRegistry.class
                    .getDeclaredField("segments");
            field.setAccessible(true);
            return (Map<SegmentId, Segment<K, V>>) field.get(registry);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read segments map for test", ex);
        }
    }
}
