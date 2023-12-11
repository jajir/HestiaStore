package com.coroptis.index.basic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.coroptis.index.ValueMerger;
import com.coroptis.index.directory.Directory;

public class SortSupportTest {

    @SuppressWarnings("unchecked")
    private final BasicIndex<Integer, String> index = mock(BasicIndex.class);

    @SuppressWarnings("unchecked")
    private final ValueMerger<Integer, String> valueMerger = mock(
            ValueMerger.class);

    private final Directory dir = mock(Directory.class);

    @Test
    void test_getFilesInRound_0_extension() throws Exception {
        final SortSupport<Integer, String> sortSupport = makeSortSupport_extension(
                "round.dat");

        final List<String> fileNames = sortSupport.getFilesInRound(0);
        assertEquals(5, fileNames.size());
        assertTrue(fileNames.contains("round-0-6.dat"));
        assertTrue(fileNames.contains("round-0-7.dat"));
        assertTrue(fileNames.contains("round-0-8.dat"));
        assertTrue(fileNames.contains("round-0-58.dat"));
        assertTrue(fileNames.contains("round-0-59.dat"));
    }

    @Test
    void test_getFilesInRound_0() throws Exception {
        final SortSupport<Integer, String> sortSupport = makeSortSupport(
                "round");

        final List<String> fileNames = sortSupport.getFilesInRound(0);
        assertEquals(5, fileNames.size());
        assertTrue(fileNames.contains("round-0-6"));
        assertTrue(fileNames.contains("round-0-7"));
        assertTrue(fileNames.contains("round-0-8"));
        assertTrue(fileNames.contains("round-0-58"));
        assertTrue(fileNames.contains("round-0-59"));
    }

    @Test
    void test_getFilesInRound_1() throws Exception {
        final SortSupport<Integer, String> sortSupport = makeSortSupport(
                "round");

        final List<String> fileNames = sortSupport.getFilesInRound(1);
        assertEquals(2, fileNames.size());
        assertTrue(fileNames.contains("round-1-0"));
        assertTrue(fileNames.contains("round-1-1"));
    }

    private SortSupport<Integer, String> makeSortSupport(final String unsortedFileName){
	when(index.getDirectory()).thenReturn(dir);
	final SortSupport<Integer, String> sortSupport = new SortSupport<>(index,valueMerger,unsortedFileName);
	final List<String> filesInDirectory = List.of("main.dat","round-0-6","round-0-7","round-0-8","round-0-58","round-0-59","round-1-0","round-1-1");
	when(dir.getFileNames()).thenReturn(filesInDirectory.stream());
	return sortSupport;
    }

    private SortSupport<Integer, String> makeSortSupport_extension(final String unsortedFileName){
	when(index.getDirectory()).thenReturn(dir);
	final SortSupport<Integer, String> sortSupport = new SortSupport<>(index,valueMerger,unsortedFileName);
	final List<String> filesInDirectory = List.of("main.dat","round-0-6.dat","round-0-7.dat","round-0-8.dat","round-0-58.dat","round-0-59.dat","round-1-0.dat","round-1-1.dat","round.dat");
	when(dir.getFileNames()).thenReturn(filesInDirectory.stream());
	return sortSupport;
    }

    @Test
    void test_makeFileName() throws Exception {
        SortSupport<Integer, String> sortSupport = makeSortSupport("round");
        assertEquals("round-1-5", sortSupport.makeFileName(1, 5));
        assertEquals("round-10-5", sortSupport.makeFileName(10, 5));

        sortSupport = makeSortSupport("round.dat");
        assertEquals("round-1-5.dat", sortSupport.makeFileName(1, 5));
        assertEquals("round-10-5.dat", sortSupport.makeFileName(10, 5));
    }

}
