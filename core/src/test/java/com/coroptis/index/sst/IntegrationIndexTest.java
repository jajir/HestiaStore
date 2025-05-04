package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.coroptis.index.LoggingContext;
import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.segment.SegmentId;

/**
 * Basic index integrations tests.
 */
public class IntegrationIndexTest extends AbstractIndexTest {
    final Directory directory = new MemDirectory();
    final SegmentId id = SegmentId.of(27);
    final TypeDescriptorString tds = new TypeDescriptorString();
    final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    private final List<Pair<Integer, String>> testData = List.of(
            Pair.of(1, "bbb"), Pair.of(2, "ccc"), Pair.of(3, "dde"),
            Pair.of(4, "ddf"), Pair.of(5, "ddg"), Pair.of(6, "ddh"),
            Pair.of(7, "ddi"), Pair.of(8, "ddj"), Pair.of(9, "ddk"),
            Pair.of(10, "ddl"), Pair.of(11, "ddm"));

    @Test
    void testBasic() throws Exception {
        final Index<Integer, String> index = makeSstIndex(false);
        writePairs(index, testData);

        /**
         * Calling of verifyIndexData before compact() will fail. It's by
         * design.
         */

        verifyIndexSearch(index, testData);
        index.compact();

        verifyIndexData(index, testData);
        verifyIndexSearch(index, testData);

        index.close();
    }

    @Test
    void test_duplicated_operations() throws Exception {
        final Index<Integer, String> index = makeSstIndex(false);
        for (int i = 0; i < 100; i++) {
            index.put(i, "kachna");
            index.delete(i);
        }
        index.compact();
        verifyIndexData(index, new ArrayList<>());
    }

    @Test
    void test_delete_search_operations() throws Exception {
        final Index<Integer, String> index = makeSstIndex(false);
        for (int i = 0; i < 300; i++) {
            index.put(i, "kachna");
            assertEquals("kachna", index.get(i));
            index.delete(i);
            assertNull(index.get(i));
            verifyIndexData(index, List.of());
        }
        verifyIndexData(index, List.of());
    }

    /**
     * In this test getStream() could ommit some results
     * 
     * @param iterations
     * @throws Exception
     */
    @ParameterizedTest
    @CsvSource(value = { "1:0", "3:3", "5:3", "15:15", "100:99",
            "102:102" }, delimiter = ':')
    void test_adds_and_deletes_operations_no_compacting(final int iterations,
            final int itemsInIndex) throws Exception {
        final Index<Integer, String> index = makeSstIndex(false);
        for (int i = 0; i < iterations; i++) {
            index.put(i, "kachna");
            assertEquals("kachna", index.get(i));
        }
        assertEquals(itemsInIndex, index.getStream().count());
        for (int i = 0; i < iterations; i++) {
            index.delete(i);
            assertNull(index.get(i));
        }
        verifyIndexData(index, List.of());
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 3, 5, 15, 100, 102 })
    void test_adds_and_deletes_operations_with_compacting(final int iterations)
            throws Exception {
        final Index<Integer, String> index = makeSstIndex(false);
        for (int i = 0; i < iterations; i++) {
            index.put(i, "kachna");
            assertEquals("kachna", index.get(i));
        }
        index.compact();
        assertEquals(iterations, index.getStream().count());
        for (int i = 0; i < iterations; i++) {
            index.delete(i);
            assertNull(index.get(i));
        }
        index.compact();
        verifyIndexData(index, List.of());
    }

    private Index<Integer, String> makeSstIndex(boolean withLog) {
        return Index.<Integer, String>builder()//
                .withDirectory(directory)//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi) //
                .withValueTypeDescriptor(tds) //
                .withCustomConf()//
                .withMaxNumberOfKeysInSegment(4) //
                .withMaxNumberOfKeysInSegmentCache(3) //
                .withMaxNumberOfKeysInSegmentIndexPage(2) //
                .withMaxNumberOfKeysInCache(2) //
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(2) //
                .withUseFullLog(withLog) //
                .withName("test_index") //
                .build();
    }

}
