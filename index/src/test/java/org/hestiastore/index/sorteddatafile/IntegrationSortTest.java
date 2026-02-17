package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.FileNameUtil;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.AbstractSegmentTest;
import org.hestiastore.index.unsorteddatafile.UnsortedDataFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IntegrationSortTest extends AbstractSegmentTest {

    private static final Random RANDOM = new Random();
    private static final TypeDescriptor<String> tds = new TypeDescriptorShortString();
    private static final TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();
    private static final String UNSORTED_FILE_NAME = "kachna.unsorted";
    private static final String SORTED_FILE_NAME = "kachna.sorted";

    private Directory dir = null;
    private UnsortedDataFile<String, Integer> unsorted = null;
    private SortedDataFile<String, Integer> sdf = null;
    private DataFileSorter<String, Integer> sorter = null;

    @BeforeEach
    void setUp() {
        dir = new MemDirectory();
        unsorted = UnsortedDataFile.<String, Integer>builder()
                .withDirectory(
                        dir)//
                .withFileName(UNSORTED_FILE_NAME)//
                .withValueWriter(tdi.getTypeWriter())//
                .withValueReader(tdi.getTypeReader())//
                .withKeyWriter(tds.getTypeWriter())//
                .withKeyReader(tds.getTypeReader())//
                .build();

        sdf = SortedDataFile.fromDirectory(
                dir,
                SORTED_FILE_NAME, tds, tdi, 1024);

        sorter = new DataFileSorter<>(unsorted, sdf,
                (k, v1, v2) -> v1 > v2 ? v1 : v2, tds, 2);
    }

    @Test
    void test_sort_3_unique_keys_shufled() {

        writeEntries(unsorted, Arrays.asList(//
                Entry.of("b", 30), //
                Entry.of("a", 20), //
                Entry.of("c", 40)));

        sorter.sort();

        verifyIteratorData(Arrays.asList(//
                Entry.of("a", 20), //
                Entry.of("b", 30), //
                Entry.of("c", 40)//
        ), sdf.openIterator());

        verifyNumberOfFiles(dir, 2);
    }

    @Test
    void test_sort_3_duplicated_keys_shufled_merged() {

        writeEntries(unsorted, Arrays.asList(//
                Entry.of("a", 30), //
                Entry.of("a", 20), //
                Entry.of("c", 40), //
                Entry.of("a", 50)));

        sorter.sort();

        verifyIteratorData(Arrays.asList(//
                Entry.of("a", 50), //
                Entry.of("c", 40)//
        ), sdf.openIterator());

        verifyNumberOfFiles(dir, 2);
    }

    @Test
    void test_sort_no_data() {
        writeEntries(unsorted, Collections.emptyList());

        sorter.sort();

        verifyIteratorData(Collections.emptyList(), sdf.openIterator());

        verifyNumberOfFiles(dir, 2);
    }

    @Test
    void test_sort_100_unique_keys_shufled() {
        final List<Entry<String, Integer>> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add(Entry.of("key" + FileNameUtil.getPaddedId(i, 3), i));
        }
        final List<Entry<String, Integer>> shufledData = new ArrayList<>(data);
        Collections.shuffle(shufledData, RANDOM);

        writeEntries(unsorted, shufledData);

        sorter.sort();

        verifyIteratorData(data, sdf.openIterator());

        verifyNumberOfFiles(dir, 2);
    }

    @Test
    void test_sort_100_duplicated_keys_shufled() {
        final List<Entry<String, Integer>> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int id = RANDOM.nextInt(10);
            data.add(Entry.of("key" + FileNameUtil.getPaddedId(id, 3), id));
        }
        final List<Entry<String, Integer>> shufledData = new ArrayList<>(data);
        Collections.shuffle(shufledData, RANDOM);

        writeEntries(unsorted, shufledData);

        sorter.sort();

        try (EntryIteratorWithCurrent<String, Integer> iterator = sdf
                .openIterator()) {
            int i = 0;
            while (iterator.hasNext()) {
                final Entry<String, Integer> entry = iterator.next();
                assertEquals("key" + FileNameUtil.getPaddedId(i, 3),
                        entry.getKey());
                i++;
            }
            assertEquals(10, i);
        }

        verifyNumberOfFiles(dir, 2);
    }

    protected <M, N> void writeEntries(final UnsortedDataFile<M, N> file,
            final List<Entry<M, N>> entries) {
        file.openWriterTx().execute(writer -> {
            for (final Entry<M, N> entry : entries) {
                writer.write(entry);
            }
        });
    }

}
