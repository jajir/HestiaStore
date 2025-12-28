package org.hestiastore.index.segment;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hestiastore.index.directory.Directory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)

class SegmentFilesRenamerTest {
    private Directory directory;

    @Mock
    private SegmentFiles<String, Long> from;

    @Mock
    private SegmentFiles<String, Long> to;

    private SegmentFilesRenamer renamer;

    @BeforeEach
    void setUp() {
        directory = mock(Directory.class);
        renamer = new SegmentFilesRenamer();
        when(from.getAsyncDirectory()).thenReturn(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(directory));
        when(from.getIndexFileName()).thenReturn("from.index");
        when(to.getIndexFileName()).thenReturn("to.index");
        when(from.getScarceFileName()).thenReturn("from.scarce");
        when(to.getScarceFileName()).thenReturn("to.scarce");
        when(from.getBloomFilterFileName()).thenReturn("from.bloom-filter");
        when(to.getBloomFilterFileName()).thenReturn("to.bloom-filter");
        when(from.getPropertiesFilename()).thenReturn("from.properties");
        when(to.getPropertiesFilename()).thenReturn("to.properties");
        when(from.getCacheFileName()).thenReturn("from.cache");
        when(to.getCacheFileName()).thenReturn("to.cache");
    }

    @Test
    void testRenameFiles() {
        renamer.renameFiles(from, to);
        verify(directory).renameFile("from.index", "to.index");
        verify(directory).renameFile("from.scarce", "to.scarce");
        verify(directory).renameFile("from.bloom-filter", "to.bloom-filter");
        verify(directory).renameFile("from.properties", "to.properties");
        verify(directory).renameFile("from.cache", "to.cache");
        verifyNoMoreInteractions(directory);
    }
}
