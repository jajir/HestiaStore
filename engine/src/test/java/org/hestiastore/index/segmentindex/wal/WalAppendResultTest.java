package org.hestiastore.index.segmentindex.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class WalAppendResultTest {

    @Test
    void exposesAppendMetadata() {
        final WalAppendResult result = new WalAppendResult(7L, 42,
                "00000000000000000007.wal");

        assertEquals(7L, result.lsn());
        assertEquals(42, result.recordBytes());
        assertEquals("00000000000000000007.wal", result.segmentName());
    }
}
