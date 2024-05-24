package com.coroptis.index.sstfile;

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

public class PairSeekableReaderImplTest {

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
        final SstFile<String, Integer> sst = new SstFile<>(dir, FILE_NAME,
                tdi.getTypeWriter(), tdi.getTypeReader(), tds.getComparator(),
                tds.getConvertorFromBytes(), tds.getConvertorToBytes());
        long position = 0;
        try (SstFileWriter<String, Integer> writer = sst.openWriter()) {
            writer.put(P1);
            writer.put(P2);
            writer.put(P3);
            position = writer.put(P4, true);
            writer.put(P5);
            writer.put(P6);
        }

        try (PairSeekableReader<String, Integer> reader = sst
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
