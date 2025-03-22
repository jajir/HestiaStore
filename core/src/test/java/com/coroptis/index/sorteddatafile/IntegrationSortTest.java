package com.coroptis.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.FileNameUtil;
import com.coroptis.index.Pair;
import com.coroptis.index.PairIteratorWithCurrent;
import com.coroptis.index.PairWriter;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.segment.AbstractSegmentTest;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;

@ExtendWith(MockitoExtension.class)
public class IntegrationSortTest extends AbstractSegmentTest {

    private final static Random RANDOM = new Random();
    private final static TypeDescriptor<String> tds = new TypeDescriptorString();
    private final static TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();
    private final static String UNSORTED_FILE_NAME = "kachna.unsorted";
    private final static String SORTED_FILE_NAME = "kachna.sorted";

    private Directory dir = null;
    private UnsortedDataFile<String, Integer> unsorted = null;
    private SortedDataFile<String, Integer> sdf = null;
    private DataFileSorter<String, Integer> sorter = null;

    @BeforeEach
    void setUp() {
        dir = new MemDirectory();
        unsorted = UnsortedDataFile.<String, Integer>builder()
                .withDirectory(dir)//
                .withFileName(UNSORTED_FILE_NAME)//
                .withValueWriter(tdi.getTypeWriter())//
                .withValueReader(tdi.getTypeReader())//
                .withKeyWriter(tds.getTypeWriter())//
                .withKeyReader(tds.getTypeReader())//
                .build();

        sdf = new SortedDataFile<>(dir, SORTED_FILE_NAME, tds, tdi, 1024);

        sorter = new DataFileSorter<>(unsorted, sdf, (k, v1, v2) -> v1 > v2 ? v1 : v2, tds, 2);
    }

    @Test
    void test_sort_3_unique_keys_shufled() throws Exception {

        writePairs(unsorted, Arrays.asList(//
                Pair.of("b", 30), //
                Pair.of("a", 20), //
                Pair.of("c", 40)));

        sorter.sort();

        verifyIteratorData(sdf.openIterator(), Arrays.asList(//
                Pair.of("a", 20), //
                Pair.of("b", 30), //
                Pair.of("c", 40)));

        verifyNumberOfFiles(dir, 2);
    }

    @Test
    void test_sort_3_duplicated_keys_shufled_merged() throws Exception {

        writePairs(unsorted, Arrays.asList(//
                Pair.of("a", 30), //
                Pair.of("a", 20), //
                Pair.of("c", 40), //
                Pair.of("a", 50)));

        sorter.sort();

        verifyIteratorData(sdf.openIterator(), Arrays.asList(//
                Pair.of("a", 50), //
                Pair.of("c", 40)));

        verifyNumberOfFiles(dir, 2);
    }

    @Test
    void test_sort_no_data() throws Exception {
        writePairs(unsorted, Collections.emptyList());

        sorter.sort();

        verifyIteratorData(sdf.openIterator(), Collections.emptyList());

        verifyNumberOfFiles(dir, 2);
    }

    @Test
    void test_sort_100_unique_keys_sorted() throws Exception {
        final List<Pair<String, Integer>> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add(Pair.of("key" + FileNameUtil.getPaddedId(i, 3), i));
        }
        final List<Pair<String, Integer>> shufledData = new ArrayList<>(data);
        Collections.shuffle(shufledData, RANDOM);

        writePairs(unsorted, shufledData);

        sorter.sort();

        verifyIteratorData(sdf.openIterator(), data);

        verifyNumberOfFiles(dir, 2);
    }

    @Test
    void test_sort_100_unique_keys_shufled() throws Exception {
        final List<Pair<String, Integer>> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add(Pair.of("key" + FileNameUtil.getPaddedId(i, 3), i));
        }
        final List<Pair<String, Integer>> shufledData = new ArrayList<>(data);
        Collections.shuffle(shufledData, RANDOM);

        writePairs(unsorted, shufledData);

        sorter.sort();

        verifyIteratorData(sdf.openIterator(), data);

        verifyNumberOfFiles(dir, 2);
    }

    @Test
    void test_sort_100_duplicated_keys_shufled() throws Exception {
        final List<Pair<String, Integer>> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int id = RANDOM.nextInt(10);
            data.add(Pair.of("key" + FileNameUtil.getPaddedId(id, 3), id));
        }
        final List<Pair<String, Integer>> shufledData = new ArrayList<>(data);
        Collections.shuffle(shufledData, RANDOM);

        writePairs(unsorted, shufledData);

        sorter.sort();

        try (PairIteratorWithCurrent<String, Integer> iterator = sdf.openIterator()) {
            int i = 0;
            while (iterator.hasNext()) {
                final Pair<String, Integer> pair = iterator.next();
                assertEquals("key" + FileNameUtil.getPaddedId(i, 3), pair.getKey());
                i++;
            }
            assertEquals(10, i);
        }

        verifyNumberOfFiles(dir, 2);
    }

    protected <M, N> void writePairs(final UnsortedDataFile<M, N> file,
            final List<Pair<M, N>> pairs) {
        try (PairWriter<M, N> writer = file.openWriter()) {
            for (final Pair<M, N> pair : pairs) {
                writer.put(pair);
            }
        }
    }

}
