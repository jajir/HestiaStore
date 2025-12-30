package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

import org.hestiastore.index.Entry;
import org.hestiastore.index.chunkentryfile.ChunkEntryFile;
import org.hestiastore.index.chunkentryfile.ChunkEntryFileWriter;
import org.hestiastore.index.chunkentryfile.ChunkEntryFileWriterTx;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentDeltaCacheWriterTest {

    @Mock
    private SegmentFiles<Integer, String> segmentFiles;
    @Mock
    private SegmentPropertiesManager propertiesManager;
    @Mock
    private SegmentResources<Integer, String> dataProvider;
    @Mock
    private ChunkEntryFile<Integer, String> deltaFile;
    @Mock
    private SegmentDeltaCache<Integer, String> segmentDeltaCache;
    @Mock
    private ChunkEntryFileWriterTx<Integer, String> writerTx;
    @Mock
    private ChunkEntryFileWriter<Integer, String> chunkWriter;

    @SuppressWarnings("unchecked")
    private void stubWriteTransactionToCaptureWrites(
            final java.util.List<Entry<Integer, String>> sink) {
        when(deltaFile.openWriterTx()).thenReturn(writerTx);
        when(segmentFiles.getDeltaCacheChunkEntryFile(any()))
                .thenReturn(deltaFile);
        when(writerTx.openWriter()).thenReturn(chunkWriter);
        org.mockito.Mockito.doAnswer(inv -> {
            final Entry<Integer, String> e = inv.getArgument(0);
            sink.add(e);
            return null;
        }).when(chunkWriter).write(any());
    }

    private SegmentDeltaCacheWriter<Integer, String> newWriter(int max) {
        return newWriter(max, 3);
    }

    private SegmentDeltaCacheWriter<Integer, String> newWriter(int max,
            int maxChunk) {
        when(segmentFiles.getKeyTypeDescriptor())
                .thenReturn(new TypeDescriptorInteger());
        return new SegmentDeltaCacheWriter<>(segmentFiles, propertiesManager,
                dataProvider, max, maxChunk);
    }

    @Test
    void constructor_validates_arguments() {
        // nulls
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentDeltaCacheWriter<>(null, propertiesManager,
                        dataProvider, 1, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentDeltaCacheWriter<>(segmentFiles, null,
                        dataProvider, 1, 1));
        // stub key TD for next call where dataProvider is null
        when(segmentFiles.getKeyTypeDescriptor())
                .thenReturn(new TypeDescriptorInteger());
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentDeltaCacheWriter<>(segmentFiles,
                        propertiesManager, null, 1, 1));

        // invalid max
        final Exception e1 = assertThrows(IllegalArgumentException.class,
                () -> new SegmentDeltaCacheWriter<>(segmentFiles,
                        propertiesManager, dataProvider, 0, 1));
        assertEquals(
                "Property 'maxNumberOfKeysInSegmentWriteCache' must be greater than 0",
                e1.getMessage());
        final Exception e2 = assertThrows(IllegalArgumentException.class,
                () -> new SegmentDeltaCacheWriter<>(segmentFiles,
                        propertiesManager, dataProvider, -1, 1));
        assertEquals(
                "Property 'maxNumberOfKeysInSegmentWriteCache' must be greater than 0",
                e2.getMessage());
        final Exception e3 = assertThrows(IllegalArgumentException.class,
                () -> new SegmentDeltaCacheWriter<>(segmentFiles,
                        propertiesManager, dataProvider, 1, 0));
        assertEquals("Property 'maxNumberOfKeysInChunk' must be greater than 0",
                e3.getMessage());
    }

    // removed public getter for configured max; validation is still covered
    // below

    @Test
    void write_increments_counter_and_updates_cache() {
        when(dataProvider.getSegmentDeltaCache()).thenReturn(segmentDeltaCache);

        final SegmentDeltaCacheWriter<Integer, String> writer = newWriter(10);
        writer.write(Entry.of(2, "B"));
        writer.write(Entry.of(1, "A"));
        assertEquals(2, writer.getNumberOfKeys());
        verify(segmentDeltaCache).put(Entry.of(2, "B"));
        verify(segmentDeltaCache).put(Entry.of(1, "A"));
    }

    @Test
    void close_writes_sorted_unique_entries_and_updates_properties() {
        when(dataProvider.getSegmentDeltaCache()).thenReturn(segmentDeltaCache);
        final java.util.List<Entry<Integer, String>> written = new java.util.ArrayList<>();
        stubWriteTransactionToCaptureWrites(written);
        when(propertiesManager.getNextDeltaFileName())
                .thenReturn("delta-name");

        final SegmentDeltaCacheWriter<Integer, String> writer = newWriter(10);
        // Unsorted with duplicates: last value wins
        writer.write(Entry.of(5, "E1"));
        writer.write(Entry.of(1, "A"));
        writer.write(Entry.of(5, "E2"));
        writer.write(Entry.of(3, "C"));
        writer.close();

        // verify order (sorted by key) and dedup
        assertEquals(3, written.size());
        assertEquals(Entry.of(1, "A"), written.get(0));
        assertEquals(Entry.of(3, "C"), written.get(1));
        assertEquals(Entry.of(5, "E2"), written.get(2));

        // properties updated by number of unique keys
        verify(propertiesManager).incrementDeltaFileNameCounter();
        verify(propertiesManager).increaseNumberOfKeysInDeltaCache(3);
        verify(chunkWriter, times(1)).flush();
        verify(writerTx).commit();
    }

    @Test
    void close_does_nothing_when_cache_is_empty() {
        final SegmentDeltaCacheWriter<Integer, String> writer = newWriter(10);

        writer.close();

        assertTrue(writer.getNumberOfKeys() == 0);
        verify(propertiesManager, never()).increaseNumberOfKeysInDeltaCache(
                anyInt());
        verify(propertiesManager, never()).incrementDeltaFileNameCounter();
        verify(segmentFiles, never()).getDeltaCacheChunkEntryFile(any());
    }

    @Test
    void controller_openWriter_constructs_writer_without_error() {
        final int max = 777;
        final SegmentDeltaCacheController<Integer, String> controller = new SegmentDeltaCacheController<>(
                segmentFiles, propertiesManager, dataProvider, max, max, 3);
        when(segmentFiles.getKeyTypeDescriptor())
                .thenReturn(new TypeDescriptorInteger());
        final SegmentDeltaCacheWriter<Integer, String> writer = controller
                .openWriter();
        assertNotNull(writer);
    }

    @Test
    void controller_openWriter_throws_for_invalid_max() {
        final int invalidMax = 0;
        final SegmentDeltaCacheController<Integer, String> controller = new SegmentDeltaCacheController<>(
                segmentFiles, propertiesManager, dataProvider, 10, invalidMax,
                3);
        assertThrows(IllegalArgumentException.class, controller::openWriter);
    }
}
