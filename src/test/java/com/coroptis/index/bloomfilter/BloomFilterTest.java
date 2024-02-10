package com.coroptis.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.MemDirectory;

public class BloomFilterTest {

    private final TypeDescriptorString STD = new TypeDescriptorString();

    private final String FILE_NAME = "segment-00880.bloomFilter";

    @Test
    void test_basic_functionality() {
        final MemDirectory directory = new MemDirectory();
        final BloomFilter<String> bf = BloomFilter.<String>builder()
                .withBloomFilterFileName(FILE_NAME)
                .withConvertorToBytes(STD.getConvertorToBytes())
                .withDirectory(directory).withIndexSizeInBytes(100)
                .withNumberOfHashFunctions(10).build();

        try (BloomFilterWriter<String> writer = bf.openWriter()) {
            assertTrue(writer.write("ahoj"));
            assertTrue(writer.write("znenku"));
            assertTrue(writer.write("karle"));
            assertTrue(writer.write("kachna"));
        }

        assertFalse(bf.isNotStored("ahoj"));
        assertFalse(bf.isNotStored("znenku"));
        assertFalse(bf.isNotStored("karle"));
        assertFalse(bf.isNotStored("kachna"));
        assertTrue(bf.isNotStored("Milan"));

        // verify statistics
        bf.getStatsString();
        final BloomFilterStats stats = bf.getStatistics();
        assertEquals(5, stats.getBloomFilterCalls());
        assertEquals(1, stats.getKeyIsNotStored());
        assertEquals(20, stats.getRatio());
    }

    @Test
    void test_default_result() {
        final MemDirectory directory = new MemDirectory();
        final BloomFilter<String> bf = BloomFilter.<String>builder()
                .withBloomFilterFileName(FILE_NAME)
                .withConvertorToBytes(STD.getConvertorToBytes())
                .withDirectory(directory).withIndexSizeInBytes(100)
                .withNumberOfHashFunctions(10).build();

        // verify statistics
        bf.getStatsString();
        final BloomFilterStats stats = bf.getStatistics();
        assertEquals(0, stats.getBloomFilterCalls());
        assertEquals(0, stats.getKeyIsNotStored());
        assertEquals(0, stats.getRatio());

        assertFalse(bf.isNotStored("hello"));
    }
}
