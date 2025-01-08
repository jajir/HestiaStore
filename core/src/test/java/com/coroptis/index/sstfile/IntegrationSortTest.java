package com.coroptis.index.sstfile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.FileNameUtil;
import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.segment.AbstractSegmentTest;
import com.coroptis.index.unsorteddatafile.IntegrationUnsortedDataFileTest;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;

@ExtendWith(MockitoExtension.class)
public class IntegrationSortTest extends AbstractSegmentTest {

    private final static Random RANDOM = new Random();
    private final static TypeDescriptor<String> tds = new TypeDescriptorString();
    private final static TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();
    private final static String UNSORTED_FILE_NAME = "kachna.unsorted";
    private final static String SORTED_FILE_NAME = "kachna.sorted";


    private final Logger logger = LoggerFactory
            .getLogger(IntegrationUnsortedDataFileTest.class);

    private Directory dir = null;
    private UnsortedDataFile<String, Integer> unsorted = null;
    private SstFile<String, Integer> sst = null;
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

        sst = new SstFile<>(dir, SORTED_FILE_NAME, tdi.getTypeWriter(),
                tdi.getTypeReader(), tds.getComparator(),
                tds.getConvertorFromBytes(), tds.getConvertorToBytes(), 1024);

        sorter = new DataFileSorter<>(unsorted, sst, (k, v1, v2) -> v1, tds, 2);
    }

    @Test
    void test_sort_3_emements() throws Exception {

        writePairs(unsorted, Arrays.asList(//
                Pair.of("b", 30), //
                Pair.of("a", 20), //
                Pair.of("c", 40)));

        sorter.sort();

        verifyIteratorData(sst.openIterator(), Arrays.asList(//
                Pair.of("a", 20), //
                Pair.of("b", 30), //
                Pair.of("c", 40)));

        verifyNumberOfFiles(dir, 2);
    }

    @Test
    void test_sort_no_data() throws Exception {
        writePairs(unsorted, Collections.emptyList());

        sorter.sort();

        verifyIteratorData(sst.openIterator(), Collections.emptyList());

        verifyNumberOfFiles(dir, 2);
    }

    @Test
    void test_sort_100_sorted() throws Exception {
        final List<Pair<String, Integer>> data = new ArrayList<>();
        for(int i=0; i<100; i++) {
            data.add(Pair.of("key" + FileNameUtil.getPaddedId(i, 3), i));
        }
        final List<Pair<String, Integer>> shufledData = new ArrayList<>(data);
        Collections.shuffle(shufledData, RANDOM);

        writePairs(unsorted, shufledData);

        sorter.sort();

        verifyIteratorData(sst.openIterator(), data);

        verifyNumberOfFiles(dir, 2);
    }

    @Test
    void test_sort_100_random() throws Exception {
        final List<Pair<String, Integer>> data = new ArrayList<>();
        for(int i=0; i<100; i++) {
            data.add(Pair.of("key" + FileNameUtil.getPaddedId(i, 3), i));
        }
        final List<Pair<String, Integer>> shufledData = new ArrayList<>(data);
        Collections.shuffle(shufledData, RANDOM);

        writePairs(unsorted, shufledData);

        sorter.sort();

        verifyIteratorData(sst.openIterator(), data);

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
