package com.coroptis.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.PairSeekableReader;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class IntegrationSortedDataFileTest {

    private final static String FILE_NAME = "pok.index";
    private final TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();
    private final TypeDescriptor<String> tds = new TypeDescriptorString();
    private final Pair<String, Integer> P1 = Pair.of("a", 1);
    private final Pair<String, Integer> P2 = Pair.of("aaaaaa", 2);
    private final Pair<String, Integer> P3 = Pair.of("bbb", 3);
    private final Pair<String, Integer> P4 = Pair.of("bbbbbb", 4);
    private final Pair<String, Integer> P5 = Pair.of("ccc", 5);
    private final Pair<String, Integer> P6 = Pair.of("ccccccc", 6);

    @Test
    void testName() throws Exception {
        final Directory dir = new MemDirectory();
        final SortedDataFile<String, Integer> sdf = new SortedDataFile<>(dir,
                FILE_NAME, tds, tdi, 1024);
        long position = 0;
        try (SortedDataFileWriter<String, Integer> writer = sdf.openWriter()) {
            writer.write(P1);
            writer.write(P2);
            writer.write(P3);
            position = writer.writeFull(P4);
            writer.write(P5);
            writer.write(P6);
        }

        try (PairSeekableReader<String, Integer> reader = sdf
                .openSeekableReader()) {
            // verify reading from the beginning
            verifyEquals(P1, reader.read());
            verifyEquals(P2, reader.read());

            // verify reading from saved position
            reader.seek(position);
            verifyEquals(P4, reader.read());
            verifyEquals(P5, reader.read());

            // verify reading from the beginning
            reader.seek(0);
            verifyEquals(P1, reader.read());
            verifyEquals(P2, reader.read());
        }

    }

    private void verifyEquals(final Pair<String, Integer> expectedPair,
            final Pair<String, Integer> pair) {
        assertNotNull(expectedPair);
        assertNotNull(pair);
        assertEquals(expectedPair.getKey(), pair.getKey());
        assertEquals(expectedPair.getValue(), pair.getValue());
    }

}
