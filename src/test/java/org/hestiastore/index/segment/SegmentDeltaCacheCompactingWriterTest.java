package org.hestiastore.index.segment;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentDeltaCacheCompactingWriterTest {

    private static final Entry<Integer, String> ENTRY_1 = Entry.of(1, "aaa");
    private static final Entry<Integer, String> ENTRY_2 = Entry.of(2, "bbb");
    private static final Entry<Integer, String> ENTRY_3 = Entry.of(3, "ccc");

    @Mock
    private SegmentDeltaCacheController<Integer, String> deltaCacheController;

    @Mock
    private SegmentDeltaCacheWriter<Integer, String> deltaCacheWriter;

    @Test
    void test_basic_writing() {
        when(deltaCacheController.openWriter()).thenReturn(deltaCacheWriter);
        try (final EntryWriter<Integer, String> writer = new SegmentDeltaCacheCompactingWriter<>(
                deltaCacheController)) {
            writer.write(ENTRY_1);
            writer.write(ENTRY_2);
            writer.write(ENTRY_3);
        }

        verify(deltaCacheController).openWriter();
        verify(deltaCacheWriter).write(ENTRY_1);
        verify(deltaCacheWriter).write(ENTRY_2);
        verify(deltaCacheWriter).write(ENTRY_3);
        verify(deltaCacheWriter).close();
    }

    @Test
    void test_writer_opened_once_for_multiple_writes() {
        when(deltaCacheController.openWriter()).thenReturn(deltaCacheWriter);
        try (final EntryWriter<Integer, String> writer = new SegmentDeltaCacheCompactingWriter<>(
                deltaCacheController)) {
            writer.write(ENTRY_1);
            writer.write(ENTRY_2);
            writer.write(ENTRY_3);
        }

        verify(deltaCacheController).openWriter();
        verify(deltaCacheWriter).write(ENTRY_1);
        verify(deltaCacheWriter).write(ENTRY_2);
        verify(deltaCacheWriter).write(ENTRY_3);
        verify(deltaCacheWriter).close();
    }

    @Test
    void test_close_without_writes_does_not_open_writer() {
        try (final EntryWriter<Integer, String> writer = new SegmentDeltaCacheCompactingWriter<>(
                deltaCacheController)) {
            // no writes
        }

        verify(deltaCacheController, never()).openWriter();
    }

}
