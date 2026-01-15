package org.hestiastore.index.segment;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentMaintenancePathTest {

    @Mock
    private SegmentFiles<Integer, String> segmentFiles;
    @Mock
    private SegmentPropertiesManager segmentPropertiesManager;
    @Mock
    private SegmentResources<Integer, String> segmentResources;
    @Mock
    private SegmentDeltaCacheController<Integer, String> deltaCacheController;
    @Mock
    private SegmentDeltaCacheWriter<Integer, String> deltaCacheWriter;

    private SegmentMaintenancePath<Integer, String> subject;

    @BeforeEach
    void setUp() {
        final SegmentConf conf = new SegmentConf(1, 1, 1, 1, 0, 0, 0.01, 1024,
                List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        subject = new SegmentMaintenancePath<>(segmentFiles, conf,
                segmentPropertiesManager, segmentResources, deltaCacheController);
    }

    @Test
    void flushFrozenWriteCacheToDeltaFile_noop_on_empty_list() {
        subject.flushFrozenWriteCacheToDeltaFile(List.of());

        verify(deltaCacheController, never()).openWriter();
    }

    @Test
    void flushFrozenWriteCacheToDeltaFile_writes_all_entries() {
        when(deltaCacheController.openWriter()).thenReturn(deltaCacheWriter);
        final List<Entry<Integer, String>> entries = List.of(
                Entry.of(1, "a"), Entry.of(2, "b"));

        subject.flushFrozenWriteCacheToDeltaFile(entries);

        verify(deltaCacheController).openWriter();
        verify(deltaCacheWriter).write(entries.get(0));
        verify(deltaCacheWriter).write(entries.get(1));
        verify(deltaCacheWriter).close();
    }

    @Test
    void flushFrozenWriteCacheToDeltaFile_noop_on_null_list() {
        subject.flushFrozenWriteCacheToDeltaFile(null);

        verify(deltaCacheController, never()).openWriter();
        verify(deltaCacheWriter, never()).write(any());
    }
}
