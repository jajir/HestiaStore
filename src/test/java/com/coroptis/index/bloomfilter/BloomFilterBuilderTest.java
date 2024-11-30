package com.coroptis.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class BloomFilterBuilderTest {

    private final static TypeDescriptor<String> TDS = new TypeDescriptorString();

    private final static String FILE_NAME = "test.bf";

    private final static String OBJECT_NAME = "segment-01940";

    private Directory directory;

    @Test
    void test_basic_functionality() {
        final BloomFilter<String> bf = BloomFilter.<String>builder()//
                .withDirectory(directory)//
                .withBloomFilterFileName(FILE_NAME)//
                .withConvertorToBytes(TDS.getConvertorToBytes())//
                .withNumberOfKeys(10001L)//
                .withProbabilityOfFalsePositive(0.0001)//
                .withIndexSizeInBytes(1024)//
                .withNumberOfHashFunctions(2)//
                .withRelatedObjectName(OBJECT_NAME)//
                .build();
        assertNotNull(bf);
        assertEquals(2, bf.getNumberOfHashFunctions());
        assertEquals(1024L, bf.getIndexSizeInBytes());
    }

    @Test
    void test_with_indexSizeInBytes_withNumberOfHashFunctions() {
        final BloomFilter<String> bf = BloomFilter.<String>builder()//
                .withDirectory(directory)//
                .withBloomFilterFileName(FILE_NAME)//
                .withConvertorToBytes(TDS.getConvertorToBytes())//
                .withIndexSizeInBytes(1024)//
                .withNumberOfHashFunctions(2)//
                .withRelatedObjectName(OBJECT_NAME)//
                .build();
        assertNotNull(bf);
        assertEquals(2, bf.getNumberOfHashFunctions());
        assertEquals(1024L, bf.getIndexSizeInBytes());
    }

    @Test
    void test_with_indexSizeInBytes_is_zero() {
        final BloomFilter<String> bf = BloomFilter.<String>builder()//
                .withDirectory(directory)//
                .withBloomFilterFileName(FILE_NAME)//
                .withConvertorToBytes(TDS.getConvertorToBytes())//
                .withIndexSizeInBytes(0)//
                .withNumberOfHashFunctions(2)//
                .withRelatedObjectName(OBJECT_NAME)//
                .build();
        assertNotNull(bf);
        assertEquals(2, bf.getNumberOfHashFunctions());
        assertEquals(0L, bf.getIndexSizeInBytes());
    }

    @Test
    void test_with_indexSizeInBytes_is_zero_numberOfHashFunctions_null() {
        final BloomFilter<String> bf = BloomFilter.<String>builder()//
                .withDirectory(directory)//
                .withBloomFilterFileName(FILE_NAME)//
                .withConvertorToBytes(TDS.getConvertorToBytes())//
                .withIndexSizeInBytes(0)//
                .withRelatedObjectName(OBJECT_NAME)//
                .build();
        assertNotNull(bf);
        assertEquals(1, bf.getNumberOfHashFunctions());
        assertEquals(0L, bf.getIndexSizeInBytes());
    }

    @Test
    void test_with_probabilityOfFalsePositive_is_null_() {
        final BloomFilter<String> bf = BloomFilter.<String>builder()//
                .withDirectory(directory)//
                .withBloomFilterFileName(FILE_NAME)//
                .withConvertorToBytes(TDS.getConvertorToBytes())//
                .withIndexSizeInBytes(1024)//
                .withNumberOfHashFunctions(2)//
                .withProbabilityOfFalsePositive(null)//
                .withRelatedObjectName(OBJECT_NAME)//
                .build();
        assertNotNull(bf);
        assertEquals(2, bf.getNumberOfHashFunctions());
        assertEquals(1024L, bf.getIndexSizeInBytes());
    }

    @Test
    void test_without_numberOfHashFunctions() {
        final BloomFilter<String> bf = BloomFilter.<String>builder()//
                .withDirectory(directory)//
                .withBloomFilterFileName(FILE_NAME)//
                .withConvertorToBytes(TDS.getConvertorToBytes())//
                .withNumberOfKeys(1000001L)//
                .withProbabilityOfFalsePositive(0.0001)//
                .withIndexSizeInBytes(1_000_000)//
                .withRelatedObjectName(OBJECT_NAME)//
                .build();
        assertNotNull(bf);
        assertEquals(1, bf.getNumberOfHashFunctions());
        assertEquals(1000000L, bf.getIndexSizeInBytes());
    }

    @Test
    void test_without_numberOfHashFunctions_indexSizeInBytes() {
        final BloomFilter<String> bf = BloomFilter.<String>builder()//
                .withDirectory(directory)//
                .withBloomFilterFileName(FILE_NAME)//
                .withConvertorToBytes(TDS.getConvertorToBytes())//
                .withNumberOfKeys(1000001L)//
                .withProbabilityOfFalsePositive(0.0001)//
                .withRelatedObjectName(OBJECT_NAME)//
                .build();
        assertNotNull(bf);
        assertEquals(14, bf.getNumberOfHashFunctions());
        assertEquals(19170135L, bf.getIndexSizeInBytes());
    }

    @Test
    void test_without_indexSizeInBytes() {
        final BloomFilter<String> bf = BloomFilter.<String>builder()//
                .withDirectory(directory)//
                .withBloomFilterFileName(FILE_NAME)//
                .withConvertorToBytes(TDS.getConvertorToBytes())//
                .withNumberOfKeys(1000001L)//
                .withProbabilityOfFalsePositive(0.0001)//
                .withNumberOfHashFunctions(2)//
                .withRelatedObjectName(OBJECT_NAME)//
                .build();
        assertNotNull(bf);
        assertEquals(2, bf.getNumberOfHashFunctions());
        assertEquals(19170135L, bf.getIndexSizeInBytes());
    }

    @Test
    void test_missing_numberOfKeys() {
        final Exception e = assertThrows(IllegalStateException.class,
                () -> BloomFilter.<String>builder()//
                        .withDirectory(directory)//
                        .withBloomFilterFileName(FILE_NAME)//
                        .withConvertorToBytes(TDS.getConvertorToBytes())//
                        .withProbabilityOfFalsePositive(0.0001)//
                        .withNumberOfHashFunctions(2)//
                        .build());

        assertNotNull(e);
        assertEquals("Number of keys is not set.", e.getMessage());
    }

    @Test
    void test_missing_relatedObjectName() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> BloomFilter.<String>builder()//
                        .withBloomFilterFileName(FILE_NAME)//
                        .withConvertorToBytes(TDS.getConvertorToBytes())//
                        .withDirectory(directory)//
                        .withIndexSizeInBytes(0)//
                        .withNumberOfHashFunctions(0)//
                        .build());

        assertEquals("Bloom filter related object name is required",
                e.getMessage());
    }

    @Test
    void test_missing_conventorToBytes() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> BloomFilter.<String>builder()//
                        .withDirectory(directory)//
                        .withBloomFilterFileName(FILE_NAME)//
                        .withProbabilityOfFalsePositive(0.0001)//
                        .withNumberOfHashFunctions(2)//
                        .build());

        assertNotNull(e);
        assertEquals("Convertor to bytes is not set.", e.getMessage());
    }

    @Test
    void test_missing_bloomFilterName() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> BloomFilter.<String>builder()//
                        .withDirectory(directory)//
                        .withProbabilityOfFalsePositive(0.0001)//
                        .withNumberOfHashFunctions(2)//
                        .build());

        assertNotNull(e);
        assertEquals("Bloom filter file name is not set.", e.getMessage());
    }

    @Test
    void test_missing_directory() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> BloomFilter.<String>builder()//
                        .withBloomFilterFileName(FILE_NAME)//
                        .withProbabilityOfFalsePositive(0.0001)//
                        .withNumberOfHashFunctions(2)//
                        .build());

        assertNotNull(e);
        assertEquals("Directory is not set.", e.getMessage());
    }

    @Test
    void test_probabilityOfFalsePositive_is_zero() {
        final Exception e = assertThrows(IllegalStateException.class,
                () -> makeFilter(0.0));

        assertNotNull(e);
        assertEquals("Probability of false positive must be greater than zero.",
                e.getMessage());
    }

    @Test
    void test_probabilityOfFalsePositive_is_less_than_zero() {
        final Exception e = assertThrows(IllegalStateException.class,
                () -> makeFilter(-10.0));

        assertNotNull(e);
        assertEquals("Probability of false positive must be greater than zero.",
                e.getMessage());
    }

    @Test
    void test_probabilityOfFalsePositive_is_one() {
        final BloomFilter<String> filter = makeFilter(1.0);

        assertNotNull(filter);
    }

    @Test
    void test_probabilityOfFalsePositive_is_greater_than_one() {
        final Exception e = assertThrows(IllegalStateException.class,
                () -> makeFilter(10.0));

        assertNotNull(e);
        assertEquals(
                "Probability of false positive must be less than one or equal to one.",
                e.getMessage());
    }

    private BloomFilter<String> makeFilter(
            final Double probabilityOfFalsePositive) {
        return BloomFilter.<String>builder()//
                .withDirectory(directory)//
                .withConvertorToBytes(TDS.getConvertorToBytes())//
                .withBloomFilterFileName(FILE_NAME)//
                .withProbabilityOfFalsePositive(probabilityOfFalsePositive)//
                .withNumberOfKeys(10001L)//
                .withNumberOfHashFunctions(2)//
                .withRelatedObjectName(OBJECT_NAME)//
                .build();
    }

    @BeforeEach
    void setup() {
        directory = new MemDirectory();
    }

}
