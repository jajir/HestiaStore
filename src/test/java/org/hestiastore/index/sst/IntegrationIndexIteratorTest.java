package org.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.Pair;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IntegrationIndexIteratorTest {

    private static final TypeDescriptorString TD_STRING = new TypeDescriptorString();
    private static final TypeDescriptorInteger TD_INTEGER = new TypeDescriptorInteger();

    private final Logger logger = LoggerFactory
            .getLogger(IntegrationIndexIteratorTest.class);

    private final Directory directory = new MemDirectory();
    private final List<Pair<Integer, String>> data = List.of(Pair.of(1, "bbb"),
            Pair.of(2, "ccc"), Pair.of(3, "dde"), Pair.of(4, "ddf"),
            Pair.of(5, "ddg"), Pair.of(6, "ddh"), Pair.of(7, "ddi"),
            Pair.of(8, "ddj"), Pair.of(9, "ddk"), Pair.of(10, "ddl"),
            Pair.of(11, "ddm"));

    @Test
    void test_simple_index_building() {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withName("test_index")//
                .build();
        final Index<Integer, String> index = Index.create(directory, conf);
        data.stream().forEach(index::put);
        index.compact();
        assertTrue(true); // Just to ensure no exceptions are thrown
    }

    @Test
    void test_string_defaults() {
        final IndexConfiguration<String, String> conf = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withName("test_index")//
                .build();
        final Index<String, String> index = Index.create(directory, conf);
        index.put("a", "a");
        index.put("b", "b");
        index.compact();
        assertTrue(true); // Just to ensure no exceptions are thrown
    }
    // TEST nkey class non existing conf

    @Test
    void testBasic() {
        final Index<Integer, String> index1 = makeSstIndex();

        data.stream().forEach(index1::put);
        index1.compact();
        logger.debug("verify that after that point no segment "
                + "is loaded into memory.");
        index1.getStream(SegmentWindow.unbounded()).forEach(pair -> {
            assertTrue(data.contains(pair));
        });

        assertEquals(data.size(),
                index1.getStream(SegmentWindow.unbounded()).count());
    }

    private Index<Integer, String> makeSstIndex() {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_INTEGER) //
                .withValueTypeDescriptor(TD_STRING) //
                .withMaxNumberOfKeysInSegment(4) //
                .withMaxNumberOfKeysInSegmentCache(3L) //
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(4L)
                .withMaxNumberOfKeysInSegmentIndexPage(1) //
                .withMaxNumberOfKeysInCache(3) //
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(4) //
                .withDiskIoBufferSizeInBytes(1024)//
                .withName("test_index")//
                .build();
        return Index.<Integer, String>create(directory, conf);
    }

}
