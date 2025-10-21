package org.hestiastore.index.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

class BloomFilterNullTest {

    private final BloomFilterNull<String> bloomFilter = new BloomFilterNull<>();

    @Test
    void getStatistics_returnsReusableInstance() {
        BloomFilterStats first = bloomFilter.getStatistics();
        BloomFilterStats second = bloomFilter.getStatistics();

        assertNotNull(first);
        assertSame(first, second,
                "Expected cached stats instance to be reused");
    }

    @Test
    void openWriter_allowsSafeOperations() {
        assertDoesNotThrow(() -> {
            BloomFilterWriterTx<String> tx = bloomFilter.openWriteTx();
            BloomFilterWriter<String> writer = tx.open();
            writer.write("foo");
            tx.commit();
        });
    }
}
