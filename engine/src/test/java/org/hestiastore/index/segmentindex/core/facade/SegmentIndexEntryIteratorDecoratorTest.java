package org.hestiastore.index.segmentindex.core.facade;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.NoSuchElementException;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.junit.jupiter.api.Test;

class SegmentIndexEntryIteratorDecoratorTest {

    @Test
    void decorateReturnsSameIteratorWhenContextLoggingIsDisabled() {
        final EntryIterator<Integer, String> iterator =
                new NoopEntryIterator();
        final SegmentIndexEntryIteratorDecorator<Integer, String> decorator =
                new SegmentIndexEntryIteratorDecorator<>(buildConf(false));

        assertSame(iterator, decorator.decorate(iterator));
    }

    @Test
    void decorateWrapsIteratorWhenContextLoggingIsEnabled() {
        final EntryIterator<Integer, String> iterator =
                new NoopEntryIterator();
        final SegmentIndexEntryIteratorDecorator<Integer, String> decorator =
                new SegmentIndexEntryIteratorDecorator<>(buildConf(true));

        assertInstanceOf(EntryIteratorLoggingContext.class,
                decorator.decorate(iterator));
    }

    private IndexConfiguration<Integer, String> buildConf(
            final boolean contextLoggingEnabled) {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("segment-index-entry-iterator-decorator-test")
                .withContextLoggingEnabled(contextLoggingEnabled)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInActivePartition(5)
                .withMaxNumberOfKeysInPartitionBuffer(6)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withMaxNumberOfKeysInSegment(100)
                .withMaxNumberOfSegmentsInCache(3)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSizeInBytes(1024)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }

    private static final class NoopEntryIterator
            implements EntryIterator<Integer, String> {

        private boolean closed;

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Entry<Integer, String> next() {
            throw new NoSuchElementException();
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean wasClosed() {
            return closed;
        }
    }
}
