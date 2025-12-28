package org.hestiastore.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.DirectoryFacade;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BloomFilterBuilderTest {

    private static final TypeDescriptor<String> TDS = new TypeDescriptorShortString();

    private static final String FILE_NAME = "test.bf";

    private static final String OBJECT_NAME = "segment-01940";

    private Directory directory;

    @Test
    void test_basic_functionality() {
        final BloomFilter<String> bf = BloomFilter.<String>builder()//
                .withDirectoryFacade(DirectoryFacade.of(directory))//
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
                .withDirectoryFacade(DirectoryFacade.of(directory))//
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
                .withDirectoryFacade(DirectoryFacade.of(directory))//
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
                .withDirectoryFacade(DirectoryFacade.of(directory))//
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
                .withDirectoryFacade(DirectoryFacade.of(directory))//
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
                .withDirectoryFacade(DirectoryFacade.of(directory))//
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
                .withDirectoryFacade(DirectoryFacade.of(directory))//
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
                .withDirectoryFacade(DirectoryFacade.of(directory))//
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
        final BloomFilter<String> filter = BloomFilter.<String>builder()//
                .withDirectoryFacade(DirectoryFacade.of(directory))//
                .withBloomFilterFileName(FILE_NAME)//
                .withConvertorToBytes(TDS.getConvertorToBytes())//
                .withProbabilityOfFalsePositive(0.0001)//
                .withNumberOfHashFunctions(2)//
                .build();

        assertNotNull(filter);
        assertTrue(filter instanceof BloomFilterNull,
                "Expected null-object BloomFilter when sizing is absent");
    }

    @Test
    void test_missing_relatedObjectName() {
        final BloomFilterBuilder<String> builder = BloomFilter.<String>builder()//
                .withBloomFilterFileName(FILE_NAME)//
                .withConvertorToBytes(TDS.getConvertorToBytes())//
                .withDirectoryFacade(DirectoryFacade.of(directory))//
                .withIndexSizeInBytes(0)//
                .withNumberOfHashFunctions(0)//
        ;
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.build());

        assertEquals("Property 'relatedObjectName' must not be null.",
                e.getMessage());
    }

    @Test
    void test_missing_conventorToBytes() {
        final BloomFilterBuilder<String> builder = BloomFilter.<String>builder()//
                .withDirectoryFacade(DirectoryFacade.of(directory))//
                .withBloomFilterFileName(FILE_NAME)//
                .withProbabilityOfFalsePositive(0.0001)//
                .withNumberOfHashFunctions(2)//
        ;
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.build());

        assertNotNull(e);
        assertEquals("Property 'convertorToBytes' must not be null.",
                e.getMessage());
    }

    @Test
    void test_missing_bloomFilterName() {
        final BloomFilterBuilder<String> builder = BloomFilter.<String>builder()//
                .withDirectoryFacade(DirectoryFacade.of(directory))//
                .withProbabilityOfFalsePositive(0.0001)//
                .withNumberOfHashFunctions(2)//
        ;
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.build());

        assertNotNull(e);
        assertEquals("Property 'bloomFilterFileName' must not be null.",
                e.getMessage());
    }

    @Test
    void test_missing_directory() {
        final BloomFilterBuilder<String> builder = BloomFilter.<String>builder()//
                .withBloomFilterFileName(FILE_NAME)//
                .withProbabilityOfFalsePositive(0.0001)//
                .withNumberOfHashFunctions(2)//
        ;
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> builder.build());

        assertNotNull(e);
        assertEquals("Property 'directoryFacade' must not be null.",
                e.getMessage());
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
                .withDirectoryFacade(DirectoryFacade.of(directory))//
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
