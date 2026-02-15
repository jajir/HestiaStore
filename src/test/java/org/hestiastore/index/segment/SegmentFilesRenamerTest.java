package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentFilesRenamerTest {

    private static final String SOURCE_SEGMENT_ID = "segment-00038";
    private static final String TARGET_SEGMENT_ID = "segment-00099";
    private static final String SOURCE_INDEX = "v01-index.sst";
    private static final String TARGET_INDEX = "v02-index.sst";
    private static final String SOURCE_SCARCE = "v01-scarce.sst";
    private static final String TARGET_SCARCE = "v02-scarce.sst";
    private static final String SOURCE_BLOOM = "v01-bloom-filter.bin";
    private static final String TARGET_BLOOM = "v02-bloom-filter.bin";
    private static final String SOURCE_PROPERTIES = "manifest.txt";
    private static final String TARGET_PROPERTIES = "manifest.txt";

    @Mock
    private AsyncDirectory asyncDirectory;

    @Mock
    private SegmentPropertiesManager propertiesManager;

    @Mock
    private SegmentFiles<Integer, Integer> sourceFiles;

    @Mock
    private SegmentFiles<Integer, Integer> targetFiles;

    private SegmentFilesRenamer renamer;

    @BeforeEach
    void setUp() {
        renamer = new SegmentFilesRenamer();
    }

    @AfterEach
    void tearDown() {
        renamer = null;
    }

    @Test
    void renameFiles_rejects_missing_arguments() {
        final IllegalArgumentException missingFrom = assertThrows(
                IllegalArgumentException.class, () -> renamer.renameFiles(null,
                        targetFiles, propertiesManager));
        assertEquals("Property 'from' must not be null.",
                missingFrom.getMessage());

        final IllegalArgumentException missingTo = assertThrows(
                IllegalArgumentException.class, () -> renamer
                        .renameFiles(sourceFiles, null, propertiesManager));
        assertEquals("Property 'to' must not be null.", missingTo.getMessage());

        final IllegalArgumentException missingProps = assertThrows(
                IllegalArgumentException.class,
                () -> renamer.renameFiles(sourceFiles, targetFiles, null));
        assertEquals("Property 'fromProperties' must not be null.",
                missingProps.getMessage());
    }

    @Test
    void renameFiles_renames_base_files_without_delta_files() {
        stubBaseFileNames();
        stubDefaultRenameSuccess();
        when(propertiesManager.getCacheDeltaFileNames()).thenReturn(List.of());

        renamer.renameFiles(sourceFiles, targetFiles, propertiesManager);

        verify(asyncDirectory).renameFileAsync(SOURCE_INDEX, TARGET_INDEX);
        verify(asyncDirectory).renameFileAsync(SOURCE_SCARCE, TARGET_SCARCE);
        verify(asyncDirectory).renameFileAsync(SOURCE_BLOOM, TARGET_BLOOM);
        verify(asyncDirectory).renameFileAsync(SOURCE_PROPERTIES,
                TARGET_PROPERTIES);
        verifyNoMoreInteractions(asyncDirectory);
    }

    @Test
    void renameFiles_renames_delta_files_with_matching_prefix() {
        stubBaseFileNames();
        stubDefaultRenameSuccess();
        when(propertiesManager.getCacheDeltaFileNames())
                .thenReturn(List.of("v01-delta-0001.cache",
                        "v01-delta-0042.cache"));

        renamer.renameFiles(sourceFiles, targetFiles, propertiesManager);

        verify(asyncDirectory).renameFileAsync("v01-delta-0001.cache",
                "v01-delta-0001.cache");
        verify(asyncDirectory).renameFileAsync("v01-delta-0042.cache",
                "v01-delta-0042.cache");
        verify(asyncDirectory).renameFileAsync(SOURCE_INDEX, TARGET_INDEX);
        verify(asyncDirectory).renameFileAsync(SOURCE_SCARCE, TARGET_SCARCE);
        verify(asyncDirectory).renameFileAsync(SOURCE_BLOOM, TARGET_BLOOM);
        verify(asyncDirectory).renameFileAsync(SOURCE_PROPERTIES,
                TARGET_PROPERTIES);
        verifyNoMoreInteractions(asyncDirectory);
    }

    @Test
    void renameFiles_allows_delta_names_without_source_prefix() {
        stubBaseFileNames();
        stubDefaultRenameSuccess();
        when(propertiesManager.getCacheDeltaFileNames())
                .thenReturn(List.of("other-segment-delta-001.cache",
                        "v01-delta-0007.cache"));

        renamer.renameFiles(sourceFiles, targetFiles, propertiesManager);

        verify(asyncDirectory).renameFileAsync("other-segment-delta-001.cache",
                "other-segment-delta-001.cache");
        verify(asyncDirectory).renameFileAsync("v01-delta-0007.cache",
                "v01-delta-0007.cache");
        verify(asyncDirectory).renameFileAsync(SOURCE_INDEX, TARGET_INDEX);
        verify(asyncDirectory).renameFileAsync(SOURCE_SCARCE, TARGET_SCARCE);
        verify(asyncDirectory).renameFileAsync(SOURCE_BLOOM, TARGET_BLOOM);
        verify(asyncDirectory).renameFileAsync(SOURCE_PROPERTIES,
                TARGET_PROPERTIES);
        verifyNoMoreInteractions(asyncDirectory);
    }

    @Test
    void renameFiles_stops_when_delta_rename_fails() {
        stubDirectory();
        stubSegmentIds();
        when(propertiesManager.getCacheDeltaFileNames())
                .thenReturn(List.of("v01-delta-0999.cache"));
        when(asyncDirectory.renameFileAsync(eq("v01-delta-0999.cache"),
                eq("v01-delta-0999.cache")))
                .thenReturn(CompletableFuture
                        .failedFuture(new IndexException("delta missing")));

        final CompletionException exception = assertThrows(
                CompletionException.class,
                () -> renamer.renameFiles(sourceFiles, targetFiles,
                        propertiesManager));
        assertTrue(exception.getCause() instanceof IndexException,
                "Expected IndexException as root cause");
        verify(asyncDirectory).renameFileAsync("v01-delta-0999.cache",
                "v01-delta-0999.cache");
        verify(asyncDirectory, never()).renameFileAsync(SOURCE_INDEX,
                TARGET_INDEX);
        verify(asyncDirectory, never()).renameFileAsync(SOURCE_SCARCE,
                TARGET_SCARCE);
        verify(asyncDirectory, never()).renameFileAsync(SOURCE_BLOOM,
                TARGET_BLOOM);
        verify(asyncDirectory, never()).renameFileAsync(SOURCE_PROPERTIES,
                TARGET_PROPERTIES);
        verifyNoMoreInteractions(asyncDirectory);
    }

    private void stubBaseFileNames() {
        stubDirectory();
        stubSegmentIds();
        when(sourceFiles.getIndexFileName()).thenReturn(SOURCE_INDEX);
        when(targetFiles.getIndexFileName()).thenReturn(TARGET_INDEX);
        when(sourceFiles.getScarceFileName()).thenReturn(SOURCE_SCARCE);
        when(targetFiles.getScarceFileName()).thenReturn(TARGET_SCARCE);
        when(sourceFiles.getBloomFilterFileName()).thenReturn(SOURCE_BLOOM);
        when(targetFiles.getBloomFilterFileName()).thenReturn(TARGET_BLOOM);
        when(sourceFiles.getPropertiesFilename()).thenReturn(SOURCE_PROPERTIES);
        when(targetFiles.getPropertiesFilename()).thenReturn(TARGET_PROPERTIES);
    }

    private void stubDirectory() {
        when(sourceFiles.getAsyncDirectory()).thenReturn(asyncDirectory);
    }

    private void stubSegmentIds() {
        when(sourceFiles.getSegmentIdName()).thenReturn(SOURCE_SEGMENT_ID);
        when(targetFiles.getSegmentIdName()).thenReturn(TARGET_SEGMENT_ID);
    }

    private void stubDefaultRenameSuccess() {
        when(asyncDirectory.renameFileAsync(anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
    }
}
