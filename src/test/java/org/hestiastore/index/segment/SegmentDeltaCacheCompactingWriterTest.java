package org.hestiastore.index.segment;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.junit.jupiter.api.BeforeEach;
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
    private SegmentImpl<Integer, String> segment;

    @Mock
    private SegmentDeltaCacheController<Integer, String> deltaCacheController;
    @Mock
    private SegmentCompactionPolicyWithManager compactionPolicy;
    @Mock
    private SegmentPropertiesManager segmentPropertiesManager;

    @Mock
    private SegmentDeltaCacheWriter<Integer, String> deltaCacheWriter;

    @BeforeEach
    void setup() {
        when(segment.getSegmentPropertiesManager())
                .thenReturn(segmentPropertiesManager);
    }

    @Test
    void test_basic_writing() {
        when(deltaCacheController.openWriter()).thenReturn(deltaCacheWriter);
        when(deltaCacheWriter.getNumberOfKeys()).thenReturn(1, 2, 3);
        when(compactionPolicy.shouldCompactDuringWriting(1)).thenReturn(false);
        when(compactionPolicy.shouldCompactDuringWriting(2)).thenReturn(false);
        when(compactionPolicy.shouldCompactDuringWriting(3)).thenReturn(false);
        when(segmentPropertiesManager.getDeltaFileCount()).thenReturn(0);
        try (final EntryWriter<Integer, String> writer = new SegmentDeltaCacheCompactingWriter<>(
                segment, deltaCacheController, compactionPolicy)) {
            writer.write(ENTRY_1);
            writer.write(ENTRY_2);
            writer.write(ENTRY_3);
        }

        // verify that writing to cache delta file name was done
        verify(deltaCacheWriter).write(ENTRY_1);
        verify(deltaCacheWriter).write(ENTRY_2);
        verify(deltaCacheWriter).write(ENTRY_3);

        verify(compactionPolicy).shouldCompactDuringWriting(1);
        verify(compactionPolicy).shouldCompactDuringWriting(2);
        verify(compactionPolicy).shouldCompactDuringWriting(3);
        verify(segment).requestOptionalCompaction();
    }

    @Test
    void test_compact_during_writing() {
        when(deltaCacheController.openWriter()).thenReturn(deltaCacheWriter);
        when(deltaCacheWriter.getNumberOfKeys()).thenReturn(1, 2, 3);
        when(compactionPolicy.shouldCompactDuringWriting(1)).thenReturn(false);
        when(compactionPolicy.shouldCompactDuringWriting(2)).thenReturn(true);
        when(compactionPolicy.shouldCompactDuringWriting(3)).thenReturn(false);
        when(segmentPropertiesManager.getDeltaFileCount()).thenReturn(0);
        try (final EntryWriter<Integer, String> writer = new SegmentDeltaCacheCompactingWriter<>(
                segment, deltaCacheController, compactionPolicy)) {
            writer.write(ENTRY_1);
            writer.write(ENTRY_2);
            writer.write(ENTRY_3);
        }

        // verify that writing to cache delta file name was done
        verify(deltaCacheWriter).write(ENTRY_1);
        verify(deltaCacheWriter).write(ENTRY_2);
        verify(deltaCacheWriter).write(ENTRY_3);

        verify(compactionPolicy).shouldCompactDuringWriting(1);
        verify(compactionPolicy).shouldCompactDuringWriting(2);
        verify(compactionPolicy).shouldCompactDuringWriting(3);

        verify(segment).requestCompaction();
        verify(segment).requestOptionalCompaction();
    }

    @Test
    void test_compact_when_delta_files_exceeded() {
        when(deltaCacheController.openWriter()).thenReturn(deltaCacheWriter);
        when(deltaCacheWriter.getNumberOfKeys()).thenReturn(1);
        when(compactionPolicy.shouldCompactDuringWriting(1)).thenReturn(false);
        when(segmentPropertiesManager.getDeltaFileCount()).thenReturn(
                SegmentDeltaCacheCompactingWriter.MAX_DELTA_FILES_BEFORE_COMPACTION
                        + 1);
        try (final EntryWriter<Integer, String> writer = new SegmentDeltaCacheCompactingWriter<>(
                segment, deltaCacheController, compactionPolicy)) {
            writer.write(ENTRY_1);
        }

        verify(deltaCacheWriter).write(ENTRY_1);
        verify(compactionPolicy).shouldCompactDuringWriting(1);
        verify(segment).forceCompact();
        verify(segment, never()).requestCompaction();
        verify(segment, never()).requestOptionalCompaction();
    }

}
