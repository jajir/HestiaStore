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
        assertEquals("segment-00001.index", layout.getIndexFileName());
        assertEquals("segment-00001-v2.index", layout.getIndexFileName(2));
        assertEquals("segment-00001.scarce", layout.getScarceFileName());
        assertEquals("segment-00001-v2.scarce", layout.getScarceFileName(2));
        assertEquals("segment-00001.bloom-filter",
                layout.getBloomFilterFileName());
        assertEquals("segment-00001-v2.bloom-filter",
                layout.getBloomFilterFileName(2));
        assertEquals("segment-00001.properties",
                layout.getPropertiesFileName());
        assertEquals("segment-00001.lock", layout.getLockFileName());
        assertEquals("segment-00001-delta-000.cache",
                layout.getDeltaCacheFileName(0));
        assertEquals("segment-00001-v2-delta-000.cache",
                layout.getDeltaCacheFileName(2, 0));
        assertEquals("segment-00001-delta-1234.cache",
                layout.getDeltaCacheFileName(1234));
        assertEquals("v2", SegmentDirectoryLayout.getVersionDirectoryName(2));
        assertEquals(2L,
                SegmentDirectoryLayout.parseVersionDirectoryName("v2"));
        assertEquals(-1L,
                SegmentDirectoryLayout.parseVersionDirectoryName("invalid"));
    }
}
