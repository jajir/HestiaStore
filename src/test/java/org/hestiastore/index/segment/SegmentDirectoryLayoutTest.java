package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentDirectoryLayoutTest {

    private SegmentDirectoryLayout layout;

    @BeforeEach
    void setUp() {
        layout = new SegmentDirectoryLayout(SegmentId.of(1));
    }

    @AfterEach
    void tearDown() {
        layout = null;
    }

    @Test
    void test_file_name_mapping() {
        assertEquals("v01-index.sst", layout.getIndexFileName());
        assertEquals("v02-index.sst", layout.getIndexFileName(2));
        assertEquals("v01-scarce.sst", layout.getScarceFileName());
        assertEquals("v02-scarce.sst", layout.getScarceFileName(2));
        assertEquals("v01-bloom-filter.bin", layout.getBloomFilterFileName());
        assertEquals("v02-bloom-filter.bin", layout.getBloomFilterFileName(2));
        assertEquals("manifest.txt", layout.getPropertiesFileName());
        assertEquals(".lock", layout.getLockFileName());
        assertEquals("v01-delta-0000.cache",
                layout.getDeltaCacheFileName(0));
        assertEquals("v02-delta-0000.cache",
                layout.getDeltaCacheFileName(2, 0));
        assertEquals("v01-delta-1234.cache",
                layout.getDeltaCacheFileName(1234));
        assertEquals("v02", SegmentDirectoryLayout.getVersionDirectoryName(2));
        assertEquals(2L,
                SegmentDirectoryLayout.parseVersionDirectoryName("v02"));
        assertEquals(-1L,
                SegmentDirectoryLayout.parseVersionDirectoryName("invalid"));
    }
}
