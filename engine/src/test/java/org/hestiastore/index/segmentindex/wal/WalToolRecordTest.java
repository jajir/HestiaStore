package org.hestiastore.index.segmentindex.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class WalToolRecordTest {

    @Test
    void exposesRecordMetadata() {
        final WalToolRecord record = new WalToolRecord(7L, 9L, "PUT", 2, 3,
                26);

        assertEquals(7L, record.offset());
        assertEquals(9L, record.lsn());
        assertEquals("PUT", record.operation());
        assertEquals(2, record.keyLen());
        assertEquals(3, record.valueLen());
        assertEquals(26, record.bodyLen());
    }
}
