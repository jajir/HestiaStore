package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.AbstractDataTest;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIteratorWithCurrent;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IntegrationSortedDataFileTest extends AbstractDataTest {

    private static final String FILE_NAME = "pok.index";
    private static final TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();
    private static final TypeDescriptor<String> tds = new TypeDescriptorShortString();
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

    private long writeDataWithOneFullWrite() {
        long position = 0;
        sdf.openWriterTx().execute(writer -> {
            writer.put(P1);
            writer.put(P2);
            writer.put(P3);
            writer.put(P4);
            writer.put(P5);
            writer.put(P6);
        });
        return position;
    }

    @Test
    void test_iterator() {
        writeDataWithOneFullWrite();

        try (PairIteratorWithCurrent<String, Integer> iterator = sdf
                .openIterator()) {

            assertTrue(iterator.getCurrent().isEmpty());

            verifyNextElement(iterator, P1);
            verifyNextElement(iterator, P2);
            verifyNextElement(iterator, P3);
            verifyNextElement(iterator, P4);
            verifyNextElement(iterator, P5);
            verifyNextElement(iterator, P6);

            assertFalse(iterator.hasNext());
            assertTrue(iterator.getCurrent().isPresent());
            verifyEquals(P6, iterator.getCurrent().get());
        }
    }

    private void verifyNextElement(
            PairIteratorWithCurrent<String, Integer> iterator,
            Pair<String, Integer> expected) {
        assertTrue(iterator.hasNext());
        verifyEquals(expected, iterator.next());
        assertTrue(iterator.getCurrent().isPresent());
        verifyEquals(expected, iterator.getCurrent().get());
    }

}
