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

    private final TypeDescriptor<String> tds = new TypeDescriptorString();

    private Directory directory;

    @Test
    void test_basic_functionality() {
        final BloomFilter<String> bf = BloomFilter.<String>builder()//
                .withDirectory(directory)//
                .withBloomFilterFileName("test.bf")//
                .withConvertorToBytes(tds.getConvertorToBytes())//
                .withNumberOfKeys(10001L)//
                .withProbabilityOfFalsePositive(0.0001)//
                .withIndexSizeInBytes(1024)//
                .withNumberOfHashFunctions(2)//
                .build();
        assertNotNull(bf);
        assertEquals(2, bf.getNumberOfHashFunctions());
        assertEquals(1024L, bf.getIndexSizeInBytes());
    }

    @Test
    void test_with_indexSizeInBytes_withNumberOfHashFunctions() {
        final BloomFilter<String> bf = BloomFilter.<String>builder()//
                .withDirectory(directory)//
                .withBloomFilterFileName("test.bf")//
                .withConvertorToBytes(tds.getConvertorToBytes())//
                .withIndexSizeInBytes(1024)//
                .withNumberOfHashFunctions(2)//
                .build();
        assertNotNull(bf);
        assertEquals(2, bf.getNumberOfHashFunctions());
        assertEquals(1024L, bf.getIndexSizeInBytes());
    }

    @Test
    void test_with_indexSizeInBytes_is_zero() {
        final BloomFilter<String> bf = BloomFilter.<String>builder()//
                .withDirectory(directory)//
                .withBloomFilterFileName("test.bf")//
                .withConvertorToBytes(tds.getConvertorToBytes())//
                .withIndexSizeInBytes(0)//
                .withNumberOfHashFunctions(2)//
                .build();
        assertNotNull(bf);
        assertEquals(2, bf.getNumberOfHashFunctions());
        assertEquals(0L, bf.getIndexSizeInBytes());
    }

    @Test
    void test_with_indexSizeInBytes_is_zero_numberOfHashFunctions_null() {
        final BloomFilter<String> bf = BloomFilter.<String>builder()//
                .withDirectory(directory)//
                .withBloomFilterFileName("test.bf")//
                .withConvertorToBytes(tds.getConvertorToBytes())//
                .withIndexSizeInBytes(0)//
                .build();
        assertNotNull(bf);
        assertEquals(1, bf.getNumberOfHashFunctions());
        assertEquals(0L, bf.getIndexSizeInBytes());
    }

    @Test
    void test_with_probabilityOfFalsePositive_is_null_() {
        final BloomFilter<String> bf = BloomFilter.<String>builder()//
                .withDirectory(directory)//
                .withBloomFilterFileName("test.bf")//
                .withConvertorToBytes(tds.getConvertorToBytes())//
                .withIndexSizeInBytes(1024)//
                .withNumberOfHashFunctions(2)//
                .withProbabilityOfFalsePositive(null)//
                .build();
        assertNotNull(bf);
        assertEquals(2, bf.getNumberOfHashFunctions());
        assertEquals(1024L, bf.getIndexSizeInBytes());
    }

    @Test
    void test_without_numberOfHashFunctions() {
        final BloomFilter<String> bf = BloomFilter.<String>builder()//
                .withDirectory(directory)//
                .withBloomFilterFileName("test.bf")//
                .withConvertorToBytes(tds.getConvertorToBytes())//
                .withNumberOfKeys(1000001L)//
                .withProbabilityOfFalsePositive(0.0001)//
                .withIndexSizeInBytes(1_000_000)//
                .build();
        assertNotNull(bf);
        assertEquals(1, bf.getNumberOfHashFunctions());
        assertEquals(1000000L, bf.getIndexSizeInBytes());
    }

    @Test
    void test_without_numberOfHashFunctions_indexSizeInBytes() {
        final BloomFilter<String> bf = BloomFilter.<String>builder()//
                .withDirectory(directory)//
                .withBloomFilterFileName("test.bf")//
                .withConvertorToBytes(tds.getConvertorToBytes())//
                .withNumberOfKeys(1000001L)//
                .withProbabilityOfFalsePositive(0.0001)//
                .build();
        assertNotNull(bf);
        assertEquals(14, bf.getNumberOfHashFunctions());
        assertEquals(19170135L, bf.getIndexSizeInBytes());
    }

    @Test
    void test_without_indexSizeInBytes() {
        final BloomFilter<String> bf = BloomFilter.<String>builder()//
                .withDirectory(directory)//
                .withBloomFilterFileName("test.bf")//
                .withConvertorToBytes(tds.getConvertorToBytes())//
                .withNumberOfKeys(1000001L)//
                .withProbabilityOfFalsePositive(0.0001)//
                .withNumberOfHashFunctions(2)//
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
                        .withBloomFilterFileName("test.bf")//
                        .withConvertorToBytes(tds.getConvertorToBytes())//
                        .withProbabilityOfFalsePositive(0.0001)//
                        .withNumberOfHashFunctions(2)//
                        .build());

        assertNotNull(e);
        assertEquals("Number of keys is not set.", e.getMessage());
    }

    @Test
    void test_missing_conventorToBytes() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> BloomFilter.<String>builder()//
                        .withDirectory(directory)//
                        .withBloomFilterFileName("test.bf")//
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
                        .withBloomFilterFileName("test.bf")//
                        .withProbabilityOfFalsePositive(0.0001)//
                        .withNumberOfHashFunctions(2)//
                        .build());

        assertNotNull(e);
        assertEquals("Directory is not set.", e.getMessage());
    }

    @BeforeEach
    void setup() {
        directory = new MemDirectory();
    }

}
