package org.hestiastore.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BloomFilterWriterTxTest {

    private static final int INDEX_SIZE_IN_BYTES = 128;
    private static final int HASH_FUNCTIONS = 3;
    private static final String FILE_NAME = "test.bloom";

    @Mock
    private ConvertorToBytes<String> convertorToBytes;

    @Mock
    private BloomFilter<String> bloomFilter;

    private BloomFilterWriterTx<String> tx;
    private Directory directory;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        tx = new BloomFilterWriterTx<>(directory, FILE_NAME, convertorToBytes,
                HASH_FUNCTIONS, INDEX_SIZE_IN_BYTES, INDEX_SIZE_IN_BYTES,
                bloomFilter);
    }

    @Test
    void cannotOpenWriterTwice() {
        tx.open();
        assertThrows(IllegalStateException.class, tx::open);
    }

    @Test
    void commitWithoutOpenFails() {
        final BloomFilterWriterTx<String> localTx = new BloomFilterWriterTx<>(
                directory, FILE_NAME, convertorToBytes, HASH_FUNCTIONS,
                INDEX_SIZE_IN_BYTES, INDEX_SIZE_IN_BYTES, bloomFilter);
        assertThrows(IllegalStateException.class, localTx::commit);
    }

    @Test
    void constructorRequiresNonNullConvertor() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new BloomFilterWriterTx<>(directory, FILE_NAME, null,
                        HASH_FUNCTIONS, INDEX_SIZE_IN_BYTES,
                        INDEX_SIZE_IN_BYTES, bloomFilter));
        assertEquals("Property 'convertorToBytes' must not be null.",
                ex.getMessage());
    }

    @Test
    void constructorRequiresNonNullBloomFilter() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new BloomFilterWriterTx<>(directory, FILE_NAME,
                        convertorToBytes, HASH_FUNCTIONS, INDEX_SIZE_IN_BYTES,
                        INDEX_SIZE_IN_BYTES, null));
        assertEquals("Property 'bloomFilter' must not be null.",
                ex.getMessage());
    }
}
