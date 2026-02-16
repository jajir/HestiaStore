package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexStateReadyTest {

    @Mock
    private FileLock fileLock;

    @Test
    void onCloseTransitionsToClosedState() {
        final IndexStateReady<Integer, String> state = new IndexStateReady<>(
                fileLock);

        final TestIndex index = new TestIndex(new MemDirectory(), new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), buildConf());
        state.onClose(index);

        assertTrue(index.getIndexState() instanceof IndexStateClosed);
        verify(fileLock).unlock();
    }

    @Test
    void tryPerformOperationDoesNothing() {
        final IndexStateReady<Integer, String> state = new IndexStateReady<>(
                fileLock);

        state.tryPerformOperation();
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withName("index-state-ready-test")//
                .withContextLoggingEnabled(false)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInSegmentWriteCache(5)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(6)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInCache(10)//
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

    private static final class TestIndex
            extends SegmentIndexImpl<Integer, String> {

        private TestIndex(
                final Directory directoryFacade,
                final TypeDescriptorInteger keyTypeDescriptor,
                final TypeDescriptorShortString valueTypeDescriptor,
                final IndexConfiguration<Integer, String> conf) {
            super(directoryFacade, keyTypeDescriptor, valueTypeDescriptor, conf);
        }
    }
}
