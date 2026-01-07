package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.Test;

class IndexInternalDefaultTest {

    @Test
    void getStreamReturnsInsertedEntries() {
        final IndexInternalDefault<Integer, String> index = new IndexInternalDefault<>(
                AsyncDirectoryAdapter.wrap(new MemDirectory()),
                new TypeDescriptorInteger(), new TypeDescriptorShortString(),
                buildConf());
        try {
            index.put(1, "one");
            index.put(2, "two");

            try (Stream<Entry<Integer, String>> stream = index
                    .getStream(SegmentWindow.unbounded())) {
                final List<Entry<Integer, String>> entries = stream.toList();
                assertEquals(2, entries.size());
                assertTrue(entries.contains(Entry.of(1, "one")));
                assertTrue(entries.contains(Entry.of(2, "two")));
            }
        } finally {
            index.close();
        }
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withName("index-internal-default-test")//
                .withContextLoggingEnabled(false)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInSegmentWriteCache(5)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringFlush(6)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInCache(10)//
                .withMaxNumberOfKeysInSegment(100)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withNumberOfCpuThreads(1)//
                .withNumberOfIoThreads(1)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }
}
