package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.bloomfilter.BloomFilterWriter;
import org.hestiastore.index.bloomfilter.BloomFilterWriterTx;
import org.hestiastore.index.chunkentryfile.ChunkEntryFileWriter;
import org.hestiastore.index.chunkstore.CellPosition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class SegmentFullWriterTest {

    @Test
    void write_flushesEveryNthEntry() {
        final SegmentResources<Integer, String> resources = mock(
                SegmentResources.class);
        @SuppressWarnings("unchecked")
        final BloomFilter<Integer> bloomFilter = (BloomFilter<Integer>) mock(
                BloomFilter.class);
        @SuppressWarnings("unchecked")
        final BloomFilterWriterTx<Integer> bloomTx = (BloomFilterWriterTx<Integer>) mock(
                BloomFilterWriterTx.class);
        @SuppressWarnings("unchecked")
        final BloomFilterWriter<Integer> bloomWriter = (BloomFilterWriter<Integer>) mock(
                BloomFilterWriter.class);
        @SuppressWarnings("unchecked")
        final ChunkEntryFileWriter<Integer, String> indexWriter = (ChunkEntryFileWriter<Integer, String>) mock(
                ChunkEntryFileWriter.class);
        @SuppressWarnings("unchecked")
        final EntryWriter<Integer, Integer> scarceWriter = (EntryWriter<Integer, Integer>) mock(
                EntryWriter.class);
        final CellPosition position = mock(CellPosition.class);

        when(resources.getBloomFilter()).thenReturn(bloomFilter);
        when(bloomFilter.openWriteTx()).thenReturn(bloomTx);
        when(bloomTx.open()).thenReturn(bloomWriter);
        when(indexWriter.flush()).thenReturn(position);
        when(position.getValue()).thenReturn(42);

        final SegmentFullWriter<Integer, String> writer = new SegmentFullWriter<>(
                2, resources, indexWriter, scarceWriter);
        try {
            writer.write(Entry.of(1, "one"));
            writer.write(Entry.of(2, "two"));

            verify(indexWriter).flush();
            final ArgumentCaptor<Entry<Integer, Integer>> captor = ArgumentCaptor
                    .forClass(Entry.class);
            verify(scarceWriter).write(captor.capture());
            assertEquals(2, captor.getValue().getKey());
            assertEquals(42, captor.getValue().getValue());
        } finally {
            writer.close();
        }
    }

    @Test
    void close_flushesRemainingDataAndCommitsBloomFilter() {
        final SegmentResources<Integer, String> resources = mock(
                SegmentResources.class);
        @SuppressWarnings("unchecked")
        final BloomFilter<Integer> bloomFilter = (BloomFilter<Integer>) mock(
                BloomFilter.class);
        @SuppressWarnings("unchecked")
        final BloomFilterWriterTx<Integer> bloomTx = (BloomFilterWriterTx<Integer>) mock(
                BloomFilterWriterTx.class);
        @SuppressWarnings("unchecked")
        final BloomFilterWriter<Integer> bloomWriter = (BloomFilterWriter<Integer>) mock(
                BloomFilterWriter.class);
        @SuppressWarnings("unchecked")
        final ChunkEntryFileWriter<Integer, String> indexWriter = (ChunkEntryFileWriter<Integer, String>) mock(
                ChunkEntryFileWriter.class);
        @SuppressWarnings("unchecked")
        final EntryWriter<Integer, Integer> scarceWriter = (EntryWriter<Integer, Integer>) mock(
                EntryWriter.class);
        final CellPosition position = mock(CellPosition.class);

        when(resources.getBloomFilter()).thenReturn(bloomFilter);
        when(bloomFilter.openWriteTx()).thenReturn(bloomTx);
        when(bloomTx.open()).thenReturn(bloomWriter);
        when(indexWriter.flush()).thenReturn(position);
        when(position.getValue()).thenReturn(7);

        final SegmentFullWriter<Integer, String> writer = new SegmentFullWriter<>(
                10, resources, indexWriter, scarceWriter);
        writer.write(Entry.of(1, "one"));
        writer.close();

        verify(indexWriter).flush();
        verify(scarceWriter).close();
        verify(indexWriter).close();
        verify(bloomWriter).close();
        verify(bloomTx).commit();
    }
}
