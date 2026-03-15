package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexStateClosingTest {

    @Mock
    private FileLock fileLock;

    @Test
    void finishCloseTransitionsToClosedStateAndUnlocks() {
        when(fileLock.isLocked()).thenReturn(true);
        final IndexStateClosing<Integer, String> state = new IndexStateClosing<>(
                fileLock);

        final TestIndex index = new TestIndex(new MemDirectory(),
                new TypeDescriptorInteger(), new TypeDescriptorShortString(),
                buildConf());
        index.setIndexState(state);

        state.finishClose(index);

        assertTrue(index.getIndexState() instanceof IndexStateClosed);
        verify(fileLock).unlock();
    }

    @Test
    void tryPerformOperationRejectsWhileClosing() {
        final IndexStateClosing<Integer, String> state = new IndexStateClosing<>(
                fileLock);

        assertThrows(IllegalStateException.class, state::tryPerformOperation);
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withName("index-state-closing-test")//
                .withContextLoggingEnabled(false)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInActivePartition(5)//
                .withMaxNumberOfKeysInPartitionBuffer(6)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInSegment(100)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withIndexWorkerThreadCount(1)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }

    private static final class TestIndex
            extends SegmentIndexImpl<Integer, String> {

        private TestIndex(final Directory directoryFacade,
                final TypeDescriptorInteger keyTypeDescriptor,
                final TypeDescriptorShortString valueTypeDescriptor,
                final IndexConfiguration<Integer, String> conf) {
            super(directoryFacade, keyTypeDescriptor, valueTypeDescriptor, conf,
                    new IndexExecutorRegistry(conf));
        }
    }
}
