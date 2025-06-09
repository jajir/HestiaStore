package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.AbstractDataTest;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIteratorWithCurrent;
import org.hestiastore.index.PairSeekableReader;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IntegrationSortedDataFileTest extends AbstractDataTest {

    private static final String FILE_NAME = "pok.index";
    private static final TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();
    private static final TypeDescriptor<String> tds = new TypeDescriptorString();
    private static final Pair<String, Integer> P1 = Pair.of("a", 1);
    private static final Pair<String, Integer> P2 = Pair.of("aaaaaa", 2);
    private static final Pair<String, Integer> P3 = Pair.of("bbb", 3);
    private static final Pair<String, Integer> P4 = Pair.of("bbbbbb", 4);
    private static final Pair<String, Integer> P5 = Pair.of("ccc", 5);
    private static final Pair<String, Integer> P6 = Pair.of("ccccccc", 6);

    private Directory dir;
    private SortedDataFile<String, Integer> sdf;

    @BeforeEach
    void setUp() {
        dir = new MemDirectory();
        sdf = new SortedDataFile<>(dir, FILE_NAME, tds, tdi, 1024);
    }

    @AfterEach
    void tearDown() {
        dir = null;
        sdf = null;
    }

    long writeDataWithOneFullWrite() {
        long position = 0;
        try (SortedDataFileWriter<String, Integer> writer = sdf.openWriter()) {
            writer.write(P1);
            writer.write(P2);
            position = writer.writeFull(P3);
            writer.write(P4);
            writer.write(P5);
            writer.write(P6);
        }
        return position;
    }

    @Test
    void test_seekInFile() {
        long position = writeDataWithOneFullWrite();

        try (PairSeekableReader<String, Integer> reader = sdf
                .openSeekableReader()) {
            // verify reading from the beginning
            verifyEquals(P1, reader.read());
            verifyEquals(P2, reader.read());

            // verify reading from saved position
            reader.seek(position);
            verifyEquals(P3, reader.read());
            verifyEquals(P4, reader.read());

            // verify reading from the beginning
            reader.seek(0);
            verifyEquals(P1, reader.read());
            verifyEquals(P2, reader.read());
        }
    }

    @Test
    void test_iterator() {
        writeDataWithOneFullWrite();

        try (PairIteratorWithCurrent<String, Integer> iterator = sdf
                .openIterator()) {

            assertTrue(iterator.getCurrent().isEmpty());
            assertTrue(iterator.hasNext());
            verifyEquals(P1, iterator.next());
            assertTrue(iterator.getCurrent().isPresent());
            verifyEquals(P1, iterator.getCurrent().get());

            assertTrue(iterator.hasNext());
            verifyEquals(P2, iterator.next());
            assertTrue(iterator.getCurrent().isPresent());
            verifyEquals(P2, iterator.getCurrent().get());

            assertTrue(iterator.hasNext());
            verifyEquals(P3, iterator.next());
            assertTrue(iterator.getCurrent().isPresent());
            verifyEquals(P3, iterator.getCurrent().get());

            assertTrue(iterator.hasNext());
            verifyEquals(P4, iterator.next());
            assertTrue(iterator.getCurrent().isPresent());
            verifyEquals(P4, iterator.getCurrent().get());

            assertTrue(iterator.hasNext());
            verifyEquals(P5, iterator.next());
            assertTrue(iterator.getCurrent().isPresent());
            verifyEquals(P5, iterator.getCurrent().get());

            assertTrue(iterator.hasNext());
            verifyEquals(P6, iterator.next());
            assertTrue(iterator.getCurrent().isPresent());
            verifyEquals(P6, iterator.getCurrent().get());

            assertFalse(iterator.hasNext());
            assertTrue(iterator.getCurrent().isPresent());
            verifyEquals(P6, iterator.getCurrent().get());
        }
    }

}
