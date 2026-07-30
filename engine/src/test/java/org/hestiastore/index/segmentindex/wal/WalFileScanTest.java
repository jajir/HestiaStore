package org.hestiastore.index.segmentindex.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

class WalFileScanTest {

    @Test
    void validScanSummarizesRecords() {
        final List<WalToolRecord> records = List.of(
                new WalToolRecord(0L, 3L, "PUT", 1, 1, 22),
                new WalToolRecord(26L, 4L, "DELETE", 1, 0, 21));

        final WalFileScan scan = WalFileScan.valid("segment.wal", 51L,
                records);

        assertEquals("segment.wal", scan.fileName());
        assertEquals(51L, scan.size());
        assertEquals(records, scan.records());
        assertEquals(2L, scan.recordCount());
        assertEquals(3L, scan.firstLsn());
        assertEquals(4L, scan.lastLsn());
        assertFalse(scan.hasInvalidTail());
    }

    @Test
    void invalidScanCarriesTailDetails() {
        final WalFileScan scan = WalFileScan.invalid("segment.wal", 8L,
                List.of(), 4L, "CRC mismatch.");

        assertTrue(scan.hasInvalidTail());
        assertEquals(4L, scan.invalidOffset());
        assertEquals("CRC mismatch.", scan.invalidReason());
    }
}
