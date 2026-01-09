package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.bloomfilter.BloomFilterWriter;
import org.hestiastore.index.bloomfilter.BloomFilterWriterTx;
import org.hestiastore.index.chunkentryfile.ChunkEntryFileWriter;
import org.hestiastore.index.chunkstore.CellPosition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentFullWriterTest {

    @Mock
    private SegmentResources<Integer, String> resources;
    @Mock
    private BloomFilter<Integer> bloomFilter;
    @Mock
    private BloomFilterWriterTx<Integer> bloomTx;
    @Mock
    private BloomFilterWriter<Integer> bloomWriter;
    @Mock
    private ChunkEntryFileWriter<Integer, String> indexWriter;
    @Mock
    private EntryWriter<Integer, Integer> scarceWriter;
    @Mock
    private CellPosition position;

    private SegmentFullWriter<Integer, String> subject;

    @BeforeEach
    void setUp() {
        when(resources.getBloomFilter()).thenReturn(bloomFilter);
        when(bloomFilter.openWriteTx()).thenReturn(bloomTx);
        when(bloomTx.open()).thenReturn(bloomWriter);
        when(indexWriter.flush()).thenReturn(position);
        when(position.getValue()).thenReturn(42);
        subject = new SegmentFullWriter<>(2, resources, indexWriter,
                scarceWriter);
    }

    @AfterEach
    void tearDown() {
        if (subject != null && !subject.wasClosed()) {
            subject.close();
        }
    }

    @Test
    void write_flushesEveryNthEntry() {
        subject.write(Entry.of(1, "one"));
        subject.write(Entry.of(2, "two"));

        verify(indexWriter).flush();
        final ArgumentCaptor<Entry<Integer, Integer>> captor = ArgumentCaptor
                .forClass(Entry.class);
        verify(scarceWriter).write(captor.capture());
        assertEquals(2, captor.getValue().getKey());
        assertEquals(42, captor.getValue().getValue());
    }

    @Test
    void close_flushesRemainingDataAndDelaysBloomFilterCommit() {
        subject.write(Entry.of(1, "one"));
        subject.close();

        verify(indexWriter).flush();
        verify(scarceWriter).close();
        verify(indexWriter).close();
        verify(bloomWriter).close();
        verify(bloomTx, never()).commit();

        subject.commitBloomFilter();

        verify(bloomTx).commit();
    }
}
