package org.hestiastore.index.sst;

import static org.hestiastore.index.datatype.NullValue.NULL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IntegrationIndexIteratorTest {

    private static final TypeDescriptorShortString TD_STRING = new TypeDescriptorShortString();
    private static final TypeDescriptorInteger TD_INTEGER = new TypeDescriptorInteger();

    private final Logger logger = LoggerFactory
            .getLogger(IntegrationIndexIteratorTest.class);

    private final Directory directory = new MemDirectory();
    private final List<Entry<Integer, String>> data = List.of(Entry.of(1, "bbb"),
            Entry.of(2, "ccc"), Entry.of(3, "dde"), Entry.of(4, "ddf"),
            Entry.of(5, "ddg"), Entry.of(6, "ddh"), Entry.of(7, "ddi"),
            Entry.of(8, "ddj"), Entry.of(9, "ddk"), Entry.of(10, "ddl"),
            Entry.of(11, "ddm"));
    private final List<Entry<Integer, NullValue>> data2 = List.of(
            Entry.of(1, NULL), Entry.of(2, NULL), Entry.of(3, NULL),
            Entry.of(4, NULL), Entry.of(5, NULL), Entry.of(6, NULL),
            Entry.of(7, NULL), Entry.of(8, NULL), Entry.of(9, NULL),
            Entry.of(10, NULL), Entry.of(11, NULL));

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
    void test_null_value() {
        final IndexConfiguration<Integer, NullValue> conf = IndexConfiguration
                .<Integer, NullValue>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(NullValue.class)//
                .withName("test_index")//
                .build();
        final Index<Integer, NullValue> index = Index.create(directory, conf);
        data2.stream().forEach(index::put);
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
        index1.getStream(SegmentWindow.unbounded()).forEach(entry -> {
            assertTrue(data.contains(entry));
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
                .withMaxNumberOfKeysInSegmentCache(3) //
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(4)
                .withMaxNumberOfKeysInSegmentChunk(1) //
                .withMaxNumberOfKeysInCache(3) //
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(4) //
                .withDiskIoBufferSizeInBytes(1024)//
                .withName("test_index")//
                .build();
        return Index.<Integer, String>create(directory, conf);
    }

}
