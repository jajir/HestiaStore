package com.coroptis.index.sst;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.segment.SegmentId;

/**
 * Basic index integrations tests.
 */
public class IndexTest extends AbstractIndexTest {
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
        final Index<Integer, String> index1 = makeSstIndex(false);
        writePairs(index1, testData);

        verifyIndexData(index1, testData);
        verifyIndexSearch(index1, testData);
        index1.compact();

        verifyIndexData(index1, testData);
        verifyIndexSearch(index1, testData);
        
        index1.close();
    }

    private Index<Integer, String> makeSstIndex(boolean withLog) {
        return Index.<Integer, String>builder()//
                .withDirectory(directory)//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi) //
                .withValueTypeDescriptor(tds) //
                .withCustomConf()//
                .withMaxNumberOfKeysInSegment(2) //
                .withMaxNumberOfKeysInSegmentCache(1) //
                .withMaxNumberOfKeysInSegmentIndexPage(2) //
                .withMaxNumberOfKeysInCache(2) //
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(2) //
                .withUseFullLog(withLog) //
                .build();
    }

}
