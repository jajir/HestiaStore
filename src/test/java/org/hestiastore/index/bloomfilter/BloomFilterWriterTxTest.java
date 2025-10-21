package org.hestiastore.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.datatype.ConvertorToBytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BloomFilterWriterTxTest {

    private static final int INDEX_SIZE_IN_BYTES = 128;
    private static final int HASH_FUNCTIONS = 3;

    @Mock
    private ConvertorToBytes<String> convertorToBytes;

    @Mock
    private BloomFilter<String> bloomFilter;

    private BloomFilterWriterTx<String> tx;

    @BeforeEach
    void setUp() {
        tx = new BloomFilterWriterTx<>(convertorToBytes, HASH_FUNCTIONS,
                INDEX_SIZE_IN_BYTES, bloomFilter);
    }

    @Test
    void commitClosesWriterAndSetsNewHash() {
        final BloomFilterWriter<String> writer = tx.open();
        when(convertorToBytes.toBytes("alpha")).thenReturn(new byte[] { 1 });

        final boolean changed = writer.write("alpha");
        assertTrue(changed, "Expected at least one bloom filter bit to flip");

        tx.commit();

        final ArgumentCaptor<Hash> captor = ArgumentCaptor.forClass(Hash.class);
        verify(bloomFilter).setNewHash(captor.capture());
        assertNotNull(captor.getValue(), "Bloom filter should receive hash snapshot");
        verify(convertorToBytes).toBytes("alpha");
    }

    @Test
    void cannotOpenWriterTwice() {
        tx.open();
        assertThrows(IllegalStateException.class, tx::open);
    }

    @Test
    void commitWithoutOpenFails() {
        final BloomFilterWriterTx<String> localTx = new BloomFilterWriterTx<>(
                convertorToBytes, HASH_FUNCTIONS, INDEX_SIZE_IN_BYTES,
                bloomFilter);
        assertThrows(IllegalStateException.class, localTx::commit);
    }

    @Test
    void constructorRequiresNonNullConvertor() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new BloomFilterWriterTx<>(null, HASH_FUNCTIONS,
                        INDEX_SIZE_IN_BYTES, bloomFilter));
        assertEquals("Property 'convertorToBytes' must not be null.",
                ex.getMessage());
    }

    @Test
    void constructorRequiresNonNullBloomFilter() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new BloomFilterWriterTx<>(convertorToBytes,
                        HASH_FUNCTIONS, INDEX_SIZE_IN_BYTES, null));
        assertEquals("Property 'bloomFilter' must not be null.",
                ex.getMessage());
    }
}
