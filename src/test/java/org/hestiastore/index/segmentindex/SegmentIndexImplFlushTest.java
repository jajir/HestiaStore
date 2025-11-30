package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.log.Log;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexImplFlushTest {

    @Mock
    private Log<Integer, String> log;

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    @Test
    void flushCacheIfNeeded_skipsFlushWhenAtOrBelowLimit() {
        final TestableIndex index = new TestableIndex(
                buildConfWithCacheLimit(2), new MemDirectory(), log);

        index.put(1, "one");
        index.put(2, "two"); // cache size == limit
        index.flushCacheIfNeeded(); // explicit call to cover method directly

        assertEquals(0, index.flushCalls);
    }

    @Test
    void flushCacheIfNeeded_triggersFlushWhenPutExceedsLimit() {
        final TestableIndex index = new TestableIndex(
                buildConfWithCacheLimit(2), new MemDirectory(), log);

        index.put(1, "one");
        index.put(2, "two");
        index.put(3, "three"); // exceeds limit

        assertEquals(1, index.flushCalls);
    }

    @Test
    void deleteUsesFlushCacheIfNeededWhenLimitExceeded() {
        final TestableIndex index = new TestableIndex(
                buildConfWithCacheLimit(1), new MemDirectory(), log);

        index.delete(1); // cache size == limit
        index.delete(2); // exceeds limit

        assertEquals(1, index.flushCalls);
    }

    private IndexConfiguration<Integer, String> buildConfWithCacheLimit(
            final int maxKeysInCache) {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withName("test-index")//
                .withMaxNumberOfKeysInSegmentCache(4)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(6)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInCache(maxKeysInCache)//
                .withMaxNumberOfKeysInReadCache(0)//
                .withMaxNumberOfKeysInSegment(10)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withContextLoggingEnabled(false)//
                .withThreadSafe(false)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }

    private final class TestableIndex extends IndexInternalDefault<Integer, String> {

        private int flushCalls = 0;

        TestableIndex(final IndexConfiguration<Integer, String> conf,
                final Directory directory, final Log<Integer, String> log) {
            super(directory, tdi, tds, conf, log);
        }

        @Override
        protected void flushCache() {
            flushCalls++;
        }
    }
}
