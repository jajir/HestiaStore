package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class SegmentPropertiesManagerTest {

    @Test
    void test_store_and_read_values() throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final SegmentPropertiesManager props = new SegmentPropertiesManager(directory, id);

        SegmentStats stats = props.getSegmentStats();
        assertEquals(0, stats.getNumberOfKeys());
        assertEquals(0, stats.getNumberOfKeysInCache());
        assertEquals(0, stats.getNumberOfKeysInIndex());
        assertEquals(0, stats.getNumberOfKeysInScarceIndex());

        assertEquals(0, props.getCacheDeltaFileNames().size());
        assertEquals("segment-00027-delta-000.cache", props.getAndIncreaseDeltaFileName());
        assertEquals(1, props.getCacheDeltaFileNames().size());

        props.setNumberOfKeysInCache(87);
        props.setNumberOfKeysInScarceIndex(132);
        props.setNumberOfKeysInIndex(1023);

        stats = props.getSegmentStats();
        assertEquals(1110, stats.getNumberOfKeys());
        assertEquals(87, stats.getNumberOfKeysInCache());
        assertEquals(1023, stats.getNumberOfKeysInIndex());
        assertEquals(132, stats.getNumberOfKeysInScarceIndex());

        assertEquals("segment-00027-delta-001.cache", props.getAndIncreaseDeltaFileName());
        assertEquals(2, props.getCacheDeltaFileNames().size());

        props.clearCacheDeltaFileNamesCouter();
        assertEquals(0, props.getCacheDeltaFileNames().size());
    }

}
