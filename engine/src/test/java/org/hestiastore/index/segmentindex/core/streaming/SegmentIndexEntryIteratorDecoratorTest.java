package org.hestiastore.index.segmentindex.core.streaming;

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
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("segment-index-entry-iterator-decorator-test"))
                .logging(logging -> logging.contextEnabled(contextLoggingEnabled))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
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
