package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.WriteTransaction.WriterFunction;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.sorteddatafile.SortedDataFile;
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
    private SegmentDataProvider<Integer, String> dataProvider;
    @Mock
    private SortedDataFile<Integer, String> deltaFile;
    @Mock
    private SegmentDeltaCache<Integer, String> segmentDeltaCache;

    @SuppressWarnings("unchecked")
    private void stubWriteTransactionToCaptureWrites(
            final java.util.List<Entry<Integer, String>> sink) {
        final org.hestiastore.index.sorteddatafile.SortedDataFileWriterTx<Integer, String> tx = mock(
                org.hestiastore.index.sorteddatafile.SortedDataFileWriterTx.class);
        final EntryWriter<Integer, String> w = mock(EntryWriter.class);
        when(deltaFile.openWriterTx()).thenReturn(tx);
        when(segmentFiles.getDeltaCacheSortedDataFile(any()))
                .thenReturn(deltaFile);
        // execute: invoke writer function with our mock writer and capture
        // calls
        org.mockito.Mockito.doAnswer(inv -> {
            final WriterFunction<Integer, String> fn = inv.getArgument(0);
            org.mockito.Mockito.doAnswer(inv2 -> {
                final Entry<Integer, String> e = inv2.getArgument(0);
                sink.add(e);
                return null;
            }).when(w).write(any());
            fn.apply(w);
            return null;
        }).when(tx).execute(any());
    }

    private SegmentDeltaCacheWriter<Integer, String> newWriter(int max) {
        when(segmentFiles.getKeyTypeDescriptor())
                .thenReturn(new TypeDescriptorInteger());
        return new SegmentDeltaCacheWriter<>(segmentFiles, propertiesManager,
                dataProvider, max);
    }

    @Test
    void constructor_validates_arguments() {
        // nulls
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentDeltaCacheWriter<>(null, propertiesManager,
                        dataProvider, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentDeltaCacheWriter<>(segmentFiles, null,
                        dataProvider, 1));
        // stub key TD for next call where dataProvider is null
        when(segmentFiles.getKeyTypeDescriptor())
                .thenReturn(new TypeDescriptorInteger());
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentDeltaCacheWriter<>(segmentFiles,
                        propertiesManager, null, 1));

        // invalid max
        final Exception e1 = assertThrows(IllegalArgumentException.class,
                () -> new SegmentDeltaCacheWriter<>(segmentFiles,
                        propertiesManager, dataProvider, 0));
        assertEquals(
                "Property 'maxNumberOfKeysInSegmentDeltaCache' must be greater than 0",
                e1.getMessage());
        final Exception e2 = assertThrows(IllegalArgumentException.class,
                () -> new SegmentDeltaCacheWriter<>(segmentFiles,
                        propertiesManager, dataProvider, -1));
        assertEquals(
                "Property 'maxNumberOfKeysInSegmentDeltaCache' must be greater than 0",
                e2.getMessage());
    }

    // removed public getter for configured max; validation is still covered
    // below

    @Test
    void write_increments_counter_and_updates_cache_when_loaded() {
        when(dataProvider.isLoaded()).thenReturn(true);
        when(dataProvider.getSegmentDeltaCache()).thenReturn(segmentDeltaCache);

        final SegmentDeltaCacheWriter<Integer, String> writer = newWriter(10);
        writer.write(Entry.of(2, "B"));
        writer.write(Entry.of(1, "A"));
        assertEquals(2, writer.getNumberOfKeys());
        verify(segmentDeltaCache).put(Entry.of(2, "B"));
        verify(segmentDeltaCache).put(Entry.of(1, "A"));
    }

    @Test
    void write_does_not_touch_cache_when_not_loaded() {
        when(dataProvider.isLoaded()).thenReturn(false);

        final SegmentDeltaCacheWriter<Integer, String> writer = newWriter(10);
        writer.write(Entry.of(3, "C"));
        // when not loaded, provider's delta cache is not queried
        verify(dataProvider, never()).getSegmentDeltaCache();
    }

    @Test
    void close_writes_sorted_unique_entries_and_updates_properties() {
        final java.util.List<Entry<Integer, String>> written = new java.util.ArrayList<>();
        stubWriteTransactionToCaptureWrites(written);
        when(propertiesManager.getAndIncreaseDeltaFileName())
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
        verify(propertiesManager).increaseNumberOfKeysInDeltaCache(3);
    }

    @Test
    void controller_openWriter_constructs_writer_without_error() {
        final int max = 777;
        final SegmentDeltaCacheController<Integer, String> controller = new SegmentDeltaCacheController<>(
                segmentFiles, propertiesManager, dataProvider, max);
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
                segmentFiles, propertiesManager, dataProvider, invalidMax);
        assertThrows(IllegalArgumentException.class, controller::openWriter);
    }
}
