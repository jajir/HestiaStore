package com.coroptis.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.MemDirectory;

public class BloomFilterTest {

    private final Logger logger = LoggerFactory
            .getLogger(BloomFilterTest.class);

    private final TypeDescriptorString STD = new TypeDescriptorString();

    private final String FILE_NAME = "segment-00880.bloomFilter";

    private MemDirectory directory = new MemDirectory();

    private final List<String> TEST_DATA_KEYS = Arrays.asList("ahoj", "znenku",
            "karle", "kachna");

    @Test
    void test_basic_functionality() {
        final BloomFilter<String> bf = makeBloomFilter();
        writeToFilter(bf, TEST_DATA_KEYS);

        assertFalse(bf.isNotStored("ahoj"));
        assertFalse(bf.isNotStored("ahoj"));
        assertFalse(bf.isNotStored("znenku"));
        assertFalse(bf.isNotStored("karle"));
        bf.incrementFalsePositive();
        assertFalse(bf.isNotStored("kachna"));
        assertTrue(bf.isNotStored("Milan"));

        // verify statistics
        bf.getStatsString();
        final BloomFilterStats stats = bf.getStatistics();
        assertEquals(6, stats.getBloomFilterCalls());
        assertEquals(1, stats.getKeyIsNotStored());
        assertEquals(5, stats.getKeyWasStored());
        assertEquals(16, stats.getRatio());
        logger.debug(stats.getStatsString());
    }

    @Test
    void test_empty_filter_stats() {
        final BloomFilter<String> bf = makeBloomFilter();
        writeToFilter(bf, TEST_DATA_KEYS);

        // verify statistics
        final BloomFilterStats stats = bf.getStatistics();
        assertEquals(0, stats.getBloomFilterCalls());
        assertEquals(0, stats.getKeyIsNotStored());
        assertEquals(0, stats.getRatio());

        logger.debug(stats.getStatsString());
        assertEquals("Bloom filter was not used.", stats.getStatsString());
    }

    private BloomFilter<String> makeBloomFilter() {
        return BloomFilter.<String>builder().withBloomFilterFileName(FILE_NAME)
                .withConvertorToBytes(STD.getConvertorToBytes())
                .withDirectory(directory).withIndexSizeInBytes(100)
                .withNumberOfHashFunctions(10).build();
    }

    private void writeToFilter(final BloomFilter<String> bf,
            final List<String> testData) {
        try (final BloomFilterWriter<String> writer = bf.openWriter()) {
            testData.forEach(key -> {
                assertTrue(writer.write(key));
            });
        }
    }

}
