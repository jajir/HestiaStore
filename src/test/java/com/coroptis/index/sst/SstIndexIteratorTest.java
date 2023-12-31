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
import com.coroptis.index.segment.SegmentId;

public class SstIndexIteratorTest {

    private final Logger logger = LoggerFactory
            .getLogger(SstIndexIteratorTest.class);

    final Directory directory = new MemDirectory();
    final SegmentId id = SegmentId.of(27);
    final TypeDescriptorString tds = new TypeDescriptorString();
    final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    @Test
    void testBasic() throws Exception {

        final Index<Integer, String> index1 = makeSstIndex();

        final List<Pair<Integer, String>> data = List.of(Pair.of(1, "bbb"),
                Pair.of(2, "ccc"), Pair.of(3, "dde"), Pair.of(4, "ddf"),
                Pair.of(5, "ddg"), Pair.of(6, "ddh"), Pair.of(7, "ddi"),
                Pair.of(8, "ddj"), Pair.of(9, "ddk"), Pair.of(10, "ddl"),
                Pair.of(11, "ddm"));
        data.stream().forEach(index1::put);
        index1.forceCompact();
        logger.debug("verify that after that point no segment "
                + "is loaded into memory.");
        index1.getStream().forEach(pair -> {
            assertTrue(data.contains(pair));
        });

        assertEquals(data.size(), index1.getStream().count());
    }

    private SstIndexImpl<Integer, String> makeSstIndex() {
        return Index.<Integer, String>builder().withDirectory(directory)
                .withKeyTypeDescriptor(tdi) //
                .withValueTypeDescriptor(tds) //
                .withMaxNumberOfKeysInSegment(2) //
                .withMaxNumberOfKeysInSegmentCache(1) //
                .withMaxNumberOfKeysInSegmentIndexPage(1) //
                .withMaxNumberOfKeysInCache(1) //
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(4) //
                .build();
    }

}
