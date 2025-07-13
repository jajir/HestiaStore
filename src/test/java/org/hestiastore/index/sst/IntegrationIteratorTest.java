package org.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.List;

import org.hestiastore.index.Pair;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IntegrationIteratorTest extends AbstractIndexTest {

    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();
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
        IndexConfiguration<String, Integer> conf = IndexConfiguration
                .<String, Integer>builder().withKeyClass(String.class)//
                .withValueClass(Integer.class)//
                .withKeyTypeDescriptor(tds) //
                .withValueTypeDescriptor(tdi) //
                .withMaxNumberOfKeysInSegment(4) //
                .withMaxNumberOfKeysInSegmentCache(100L) //
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(200L)//
                .withMaxNumberOfKeysInSegmentIndexPage(1000) //
                .withMaxNumberOfKeysInCache(3) //
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(4) //
                .withLogEnabled(false) //
                .withName("test_index") //
                .build();
        index = Index.<String, Integer>create(directory, conf);

        writePairs(index, indexFile);
        index.compact();
    }

    @Test
    void test_case_1_simple_read() {
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
    void test_case_2_deleted_key() {
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
    void test_case_3_modify_key() {
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
    void test_case_4_add_key() {
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
    void test_case_5_flush_make_data_iterable() {
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
