package org.hestiastore.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BloomFilterTest {

    private final Logger logger = LoggerFactory
            .getLogger(BloomFilterTest.class);

    private static final TypeDescriptorShortString STD = new TypeDescriptorShortString();

    private static final String FILE_NAME = "segment-00880.bloomFilter";

    private static final List<String> TEST_DATA_KEYS = Arrays.asList("ahoj",
            "znenku", "karle", "kachna");

    private MemDirectory directory = new MemDirectory();

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
        final BloomFilterStats stats = bf.getStatistics();
        assertEquals(6, stats.getBloomFilterCalls());
        assertEquals(1, stats.getKeyIsNotStored());
        assertEquals(5, stats.getKeyWasStored());
        assertEquals(16, (int) stats.getPercentageOfFalseResponses());
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
        assertEquals(0, stats.getPercentageOfFalseResponses());

        logger.debug(stats.getStatsString());
        assertEquals("Bloom filter was not used.", stats.getStatsString());
    }

    @Test
    void test_zero_hashFuntions() {
        final BloomFilterBuilder<String> builder = BloomFilter.<String>builder()//
                .withBloomFilterFileName(FILE_NAME)//
                .withConvertorToBytes(STD.getConvertorToBytes())//
                .withAsyncDirectory(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(directory))//
                .withIndexSizeInBytes(0)//
                .withNumberOfHashFunctions(0)//
                .withRelatedObjectName("segment-00323")//
        ;
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.build());

        assertEquals("Number of hash function cant be '0'", e.getMessage());
    }

    @Test
    void test_zero_keys() {
        final BloomFilter<String> bf = BloomFilter.<String>builder()//
                .withBloomFilterFileName(FILE_NAME)//
                .withConvertorToBytes(STD.getConvertorToBytes())//
                .withAsyncDirectory(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(directory))//
                .withIndexSizeInBytes(0)//
                .withNumberOfHashFunctions(2)//
                .withRelatedObjectName("segment-00323")//
                .build();

        writeToFilter(bf, TEST_DATA_KEYS);

        // any key should be not be stored in filter, so it could be in index
        assertFalse(bf.isNotStored("ahoj"));
        assertFalse(bf.isNotStored("ahoj"));
        assertFalse(bf.isNotStored("znenku"));
        assertFalse(bf.isNotStored("karle"));
        assertFalse(bf.isNotStored("kachna"));
    }

    @Test
    void test_zero_keys_write_keys() {
        final BloomFilterBuilder<String> builder = BloomFilter.<String>builder()
                .withBloomFilterFileName(FILE_NAME)//
                .withConvertorToBytes(STD.getConvertorToBytes())//
                .withAsyncDirectory(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(directory))//
                .withIndexSizeInBytes(0)//
                .withRelatedObjectName("segment-00323")//
                .withNumberOfHashFunctions(3)//
        ;

        final BloomFilter<String> bf1 = builder.build();
        writeToFilter(bf1, TEST_DATA_KEYS);

        final BloomFilter<String> bf2 = builder.build();

        // any key should be not be stored in filter, so it could be in index
        assertFalse(bf2.isNotStored("ahoj"));
        assertFalse(bf2.isNotStored("ahoj"));
        assertFalse(bf2.isNotStored("znenku"));
        assertFalse(bf2.isNotStored("karle"));
        assertFalse(bf2.isNotStored("kachna"));
    }

    private BloomFilter<String> makeBloomFilter() {
        return BloomFilter.<String>builder()//
                .withBloomFilterFileName(FILE_NAME)//
                .withConvertorToBytes(STD.getConvertorToBytes())//
                .withAsyncDirectory(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(directory))//
                .withIndexSizeInBytes(100)//
                .withNumberOfHashFunctions(2)//
                .withRelatedObjectName("segment-00323")//
                .build();
    }

    private void writeToFilter(final BloomFilter<String> bf,
            final List<String> testData) {
        final BloomFilterWriterTx<String> tx = bf.openWriteTx();
        try (final BloomFilterWriter<String> writer = tx.open()) {
            testData.forEach(key -> assertTrue(writer.write(key)));
        }
        tx.commit();
    }

}
