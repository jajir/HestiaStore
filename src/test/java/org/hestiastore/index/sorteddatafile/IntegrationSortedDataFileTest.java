package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.AbstractDataTest;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorWithCurrent;
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
    private static final Entry<String, Integer> P1 = Entry.of("a", 1);
    private static final Entry<String, Integer> P2 = Entry.of("aaaaaa", 2);
    private static final Entry<String, Integer> P3 = Entry.of("bbb", 3);
    private static final Entry<String, Integer> P4 = Entry.of("bbbbbb", 4);
    private static final Entry<String, Integer> P5 = Entry.of("ccc", 5);
    private static final Entry<String, Integer> P6 = Entry.of("ccccccc", 6);

    private Directory dir;
    private SortedDataFile<String, Integer> sdf;

    @BeforeEach
    void setUp() {
        dir = new MemDirectory();
        sdf = SortedDataFile.fromAsyncDirectory(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(dir),
                FILE_NAME, tds, tdi, 1024);
    }

    @AfterEach
    void tearDown() {
        dir = null;
        sdf = null;
    }

    private long writeDataWithOneFullWrite() {
        long position = 0;
        sdf.openWriterTx().execute(writer -> {
            writer.write(P1);
            writer.write(P2);
            writer.write(P3);
            writer.write(P4);
            writer.write(P5);
            writer.write(P6);
        });
        return position;
    }

    @Test
    void test_iterator() {
        writeDataWithOneFullWrite();

        try (EntryIteratorWithCurrent<String, Integer> iterator = sdf
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
            EntryIteratorWithCurrent<String, Integer> iterator,
            Entry<String, Integer> expected) {
        assertTrue(iterator.hasNext());
        verifyEquals(expected, iterator.next());
        assertTrue(iterator.getCurrent().isPresent());
        verifyEquals(expected, iterator.getCurrent().get());
    }

}
