package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IndexInternalConcurrentTest {

    private IndexInternalConcurrent<Integer, String> index;

    @BeforeEach
    void setUp() {
        index = new IndexInternalConcurrent<>(
                new MemDirectory(),
                new TypeDescriptorInteger(), new TypeDescriptorShortString(),
                buildConf());
    }

    @AfterEach
    void tearDown() {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
    }

    @Test
    void putGetAndStreamRoundTrip() {
        index.put(1, "one");
        index.put(2, "two");

        assertEquals("one", index.get(1));

        try (Stream<Entry<Integer, String>> stream = index
                .getStream(SegmentWindow.unbounded())) {
            final List<Entry<Integer, String>> entries = stream.toList();
            assertEquals(2, entries.size());
            assertTrue(entries.contains(Entry.of(1, "one")));
        }
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withName("index-internal-concurrent-test")//
                .withContextLoggingEnabled(false)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInSegmentWriteCache(5)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(6)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInSegment(100)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withIndexWorkerThreadCount(1)//
                .withNumberOfIoThreads(1)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }

    @Test
    void getStreamFullIsolationIsLazyAndClosesIterator() {
        final AtomicInteger hasNextCalls = new AtomicInteger();
        final AtomicInteger nextCalls = new AtomicInteger();
        final AtomicInteger closeCalls = new AtomicInteger();
        final EntryIterator<Integer, String> iterator = new CountingIterator<>(
                List.of(Entry.of(1, "one")).iterator(), hasNextCalls, nextCalls,
                closeCalls);

        final RecordingIndex streamingIndex = new RecordingIndex(iterator,
                buildConf());
        try (Stream<Entry<Integer, String>> stream = streamingIndex.getStream(
                SegmentWindow.unbounded(),
                SegmentIteratorIsolation.FULL_ISOLATION)) {
            assertEquals(SegmentIteratorIsolation.FULL_ISOLATION,
                    streamingIndex.getLastIsolation());
            assertEquals(0, hasNextCalls.get());
            assertEquals(0, nextCalls.get());
            assertEquals(1, stream.count());
        } finally {
            streamingIndex.close();
        }

        assertEquals(1, closeCalls.get());
        assertTrue(hasNextCalls.get() > 0);
        assertTrue(nextCalls.get() > 0);
    }

    private static final class RecordingIndex
            extends IndexInternalConcurrent<Integer, String> {

        private final EntryIterator<Integer, String> iterator;
        private SegmentIteratorIsolation lastIsolation;

        private RecordingIndex(final EntryIterator<Integer, String> iterator,
                final IndexConfiguration<Integer, String> conf) {
            super(new MemDirectory(),
                    new TypeDescriptorInteger(), new TypeDescriptorShortString(),
                    conf);
            this.iterator = iterator;
        }

        @Override
        public EntryIterator<Integer, String> openSegmentIterator(
                final SegmentWindow segmentWindow,
                final SegmentIteratorIsolation isolation) {
            lastIsolation = isolation;
            return iterator;
        }

        SegmentIteratorIsolation getLastIsolation() {
            return lastIsolation;
        }
    }

    private static final class CountingIterator<K, V>
            extends AbstractCloseableResource implements EntryIterator<K, V> {

        private final Iterator<Entry<K, V>> iterator;
        private final AtomicInteger hasNextCalls;
        private final AtomicInteger nextCalls;
        private final AtomicInteger closeCalls;

        private CountingIterator(final Iterator<Entry<K, V>> iterator,
                final AtomicInteger hasNextCalls,
                final AtomicInteger nextCalls,
                final AtomicInteger closeCalls) {
            this.iterator = iterator;
            this.hasNextCalls = hasNextCalls;
            this.nextCalls = nextCalls;
            this.closeCalls = closeCalls;
        }

        @Override
        public boolean hasNext() {
            hasNextCalls.incrementAndGet();
            return iterator.hasNext();
        }

        @Override
        public Entry<K, V> next() {
            nextCalls.incrementAndGet();
            return iterator.next();
        }

        @Override
        protected void doClose() {
            closeCalls.incrementAndGet();
        }
    }
}
