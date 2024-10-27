package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class IndexIteratorConsistencyTest extends AbstractIndexTest {

    private final TypeDescriptorString tds = new TypeDescriptorString();
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    private Directory directory;
    private Index<String, Integer> index;

    private final List<Pair<String, Integer>> indexFile = Arrays.asList(//
            Pair.of("a", 20), //
            Pair.of("b", 30), //
            Pair.of("c", 40));

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        index = Index.<String, Integer>builder()//
                .withDirectory(directory)//
                .withKeyClass(String.class)//
                .withValueClass(Integer.class)//
                .withKeyTypeDescriptor(tds) //
                .withValueTypeDescriptor(tdi) //
                .withCustomConf()//
                .withMaxNumberOfKeysInSegment(4) //
                .withMaxNumberOfKeysInSegmentCache(10000) //
                .withMaxNumberOfKeysInSegmentIndexPage(1000) //
                .withMaxNumberOfKeysInCache(2) //
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(4) //
                .withUseFullLog(false) //
                .build();

        writePairs(index, indexFile);
        index.compact();
    }

    @Test
    void test_case_1_simple_read() throws Exception {
        verifyIndexSearch(index, Arrays.asList(//
                Pair.of("a", 20), //
                Pair.of("b", 30), //
                Pair.of("c", 40) //
        ));
        verifyIndexData(index, Arrays.asList(//
                Pair.of("a", 20), //
                Pair.of("b", 30), //
                Pair.of("c", 40) //
        ));
    }

    @Test
    void test_case_2_deleted_key() throws Exception {
        index.delete("b");

        verifyIndexSearch(index, Arrays.asList(//
                Pair.of("a", 20), //
                Pair.of("c", 40) //
        ));
        assertNull(index.get("b"));

        verifyIndexData(index, Arrays.asList(//
                Pair.of("a", 20), //
                Pair.of("c", 40) //
        ));
    }

    @Test
    void test_case_3_modify_key() throws Exception {
        index.delete("b");
        index.put("e", 28);

        verifyIndexSearch(index, Arrays.asList(//
                Pair.of("a", 20), //
                Pair.of("e", 28), //
                Pair.of("c", 40) //
        ));
        assertNull(index.get("b"));

        verifyIndexData(index, Arrays.asList(//
                Pair.of("a", 20), //
                Pair.of("c", 40) //
        ));
    }

    @Test
    void test_case_4_add_key() throws Exception {
        index.put("g", 13);

        // verify that added value could be get by key
        verifyIndexSearch(index, Arrays.asList(//
                Pair.of("a", 20), //
                Pair.of("b", 30), //
                Pair.of("c", 40), //
                Pair.of("g", 13) //
        ));

        // verify that added value is not in iterator
        verifyIndexData(index, Arrays.asList(//
                Pair.of("a", 20), //
                Pair.of("b", 30), //
                Pair.of("c", 40)//
        ));
    }

    @Test
    void test_case_5_flush_make_data_iterable() throws Exception {
        index.delete("b");
        index.put("g", 13);
        index.flush();

        // verify data consistency after flush
        verifyIndexSearch(index, Arrays.asList(//
                Pair.of("a", 20), //
                Pair.of("c", 40), //
                Pair.of("g", 13) //
        ));

        // verify that data are in iterator after flush
        verifyIndexData(index, Arrays.asList(//
                Pair.of("a", 20), //
                Pair.of("c", 40), //
                Pair.of("g", 13)//
        ));
    }

}
