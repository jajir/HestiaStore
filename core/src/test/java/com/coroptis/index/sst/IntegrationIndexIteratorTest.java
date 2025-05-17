package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class IntegrationIndexIteratorTest {

    private final Logger logger = LoggerFactory
            .getLogger(IntegrationIndexIteratorTest.class);

    private final Directory directory = new MemDirectory();
    private final TypeDescriptorString tds = new TypeDescriptorString();
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final List<Pair<Integer, String>> data = List.of(Pair.of(1, "bbb"),
            Pair.of(2, "ccc"), Pair.of(3, "dde"), Pair.of(4, "ddf"),
            Pair.of(5, "ddg"), Pair.of(6, "ddh"), Pair.of(7, "ddi"),
            Pair.of(8, "ddj"), Pair.of(9, "ddk"), Pair.of(10, "ddl"),
            Pair.of(11, "ddm"));

    @Test
    void test_simple_index_building() throws Exception {
        final Index<Integer, String> index = Index.<Integer, String>builder()//
                .withDirectory(directory)//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withName("test_index")//
                .build();
        data.stream().forEach(index::put);
        index.compact();

    }

    @Test
    void testBasic() throws Exception {
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
        return Index.<Integer, String>builder()//
                .withDirectory(directory)//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi) //
                .withValueTypeDescriptor(tds) //
                .withCustomConf()//
                .withMaxNumberOfKeysInSegment(4) //
                .withMaxNumberOfKeysInSegmentCache(3) //
                .withMaxNumberOfKeysInSegmentIndexPage(1) //
                .withMaxNumberOfKeysInCache(1) //
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(4) //
                .withDiskIoBufferSizeInBytes(1024)//
                .withName("test_index")//
                .build();
    }

}
