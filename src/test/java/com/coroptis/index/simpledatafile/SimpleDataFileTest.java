package com.coroptis.index.simpledatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.CloseablePairReader;
import com.coroptis.index.PairWriter;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class SimpleDataFileTest {

    private final TypeDescriptorInteger intTd = new TypeDescriptorInteger();
    private final TypeDescriptorString stringTd = new TypeDescriptorString();

    @Test
    public void test_simple_merging() throws Exception {
        final Directory directory = new MemDirectory();
        final SimpleDataFile<Integer, String> sdf = new SimpleDataFile<>(
                directory, "sdf", intTd, stringTd, (k, v1, v2) -> v2);

        assertEquals(0, sdf.getStats().getNumberOfPairsInCache());
        assertEquals(0, sdf.getStats().getNumberOfPairsInMainFile());

        try (PairWriter<Integer, String> writer = sdf.openCacheWriter()) {
            writer.put(Pair.of(3, "kachna"));
            writer.put(Pair.of(1, "prase"));
            writer.put(Pair.of(5, "osel"));
        }

        assertEquals(3, sdf.getStats().getNumberOfPairsInCache());
        assertEquals(0, sdf.getStats().getNumberOfPairsInMainFile());

        sdf.compact();

        assertEquals(0, sdf.getStats().getNumberOfPairsInCache());
        assertEquals(3, sdf.getStats().getNumberOfPairsInMainFile());
    }

    @Test
    public void test_appending_data() throws Exception {
        final Directory directory = new MemDirectory();
        final SimpleDataFile<Integer, String> sdf = new SimpleDataFile<>(
                directory, "sdf", intTd, stringTd, (k, v1, v2) -> v2);

        assertEquals(0, sdf.getStats().getNumberOfPairsInCache());
        assertEquals(0, sdf.getStats().getNumberOfPairsInMainFile());

        try (PairWriter<Integer, String> writer = sdf.openCacheWriter()) {
            writer.put(Pair.of(3, "kachna"));
            writer.put(Pair.of(1, "prase"));
            writer.put(Pair.of(5, "osel"));
        }

        assertEquals(3, sdf.getStats().getNumberOfPairsInCache());
        assertEquals(0, sdf.getStats().getNumberOfPairsInMainFile());

        try (PairWriter<Integer, String> writer = sdf.openCacheWriter()) {
            writer.put(Pair.of(16, "kun"));
            writer.put(Pair.of(13, "liska"));
            writer.put(Pair.of(17, "nartoun"));
        }

        assertEquals(6, sdf.getStats().getNumberOfPairsInCache());
        assertEquals(0, sdf.getStats().getNumberOfPairsInMainFile());

        try (CloseablePairReader<Integer, String> reader = sdf.openReader()) {
            assertEquals(Pair.of(1, "prase"), reader.read());
            assertEquals(Pair.of(3, "kachna"), reader.read());
            assertEquals(Pair.of(5, "osel"), reader.read());
            assertEquals(Pair.of(13, "liska"), reader.read());
            assertEquals(Pair.of(16, "kun"), reader.read());
            assertEquals(Pair.of(17, "nartoun"), reader.read());
        }
    }

}
