package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.Test;

class IndexConfiguratonStorageTest {

    @Test
    void existsReflectsConfigurationPresence() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguratonStorage<String, String> storage = new IndexConfiguratonStorage<>(
                AsyncDirectoryAdapter.wrap(directory));

        assertFalse(storage.exists());

        storage.save(buildConf());

        assertTrue(storage.exists());
        assertEquals("index-config-storage-test",
                storage.load().getIndexName());
    }

    private IndexConfiguration<String, String> buildConf() {
        final TypeDescriptorShortString typeDescriptor = new TypeDescriptorShortString();
        return IndexConfiguration.<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(typeDescriptor)//
                .withValueTypeDescriptor(typeDescriptor)//
                .withName("index-config-storage-test")//
                .withMaxNumberOfKeysInSegmentCache(4)//
                .withMaxNumberOfKeysInSegmentWriteCache(2)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(3)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInCache(10)//
                .withMaxNumberOfKeysInSegment(10)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withIndexWorkerThreadCount(1)//
                .withNumberOfIoThreads(1)//
                .withContextLoggingEnabled(false)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }
}
