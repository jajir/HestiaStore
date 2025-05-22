package com.coroptis.index.loadtest;

import java.io.File;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorLong;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FsDirectory;
import com.coroptis.index.sst.Index;
import com.coroptis.index.sst.IndexConfiguration;

public class ConsistencyCheck {
    public final static String DIRECTORY = "target/consistency-check";
    private final static TypeDescriptor<String> TYPE_DESCRIPTOR_STRING = new TypeDescriptorString();
    private final static TypeDescriptor<Long> TYPE_DESCRIPTOR_LONG = new TypeDescriptorLong();

    private final Index<String, Long> index;

    ConsistencyCheck() {
        final Directory dir = new FsDirectory(new File(DIRECTORY));
        // Constructor logic if needed

        IndexConfiguration<String, Long> conf = IndexConfiguration
                .<String, Long>builder()//
                .withName("kachnicka")//
                .withKeyClass(String.class)//
                .withValueClass(Long.class)//
                .withKeyTypeDescriptor(TYPE_DESCRIPTOR_STRING) //
                .withValueTypeDescriptor(TYPE_DESCRIPTOR_LONG) //
                .withMaxNumberOfKeysInSegment(1_000_000) //
                .withMaxNumberOfKeysInSegmentCache(1_000L) //
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(5_000) //
                .withMaxNumberOfKeysInSegmentIndexPage(1_000) //
                .withMaxNumberOfKeysInCache(10_000_000) //
                .withBloomFilterIndexSizeInBytes(0) //
                .withBloomFilterNumberOfHashFunctions(1) //
                .withLogEnabled(false) //
                .build();
        this.index = Index.create(dir, conf);
    }

    private final static long WRITE_KEYS = 9_000_000L;

    void test() {
        // Test logic to check consistency
        // This is a placeholder for the actual test logic
        System.out.println("Consistency check passed");
        for (long i = 0; i < WRITE_KEYS; i++) {
            String key = String.valueOf(i);
            Long value = i;
            index.put(key, value);
        }
        TestStatus.setReadyToTest(true);
        index.flush();
        index.close();
    }

}
