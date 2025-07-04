package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentPropertiesManagerTest {

    private final SegmentId id = SegmentId.of(27);
    private Directory directory;
    private SegmentPropertiesManager props;

    @Test
    void test_store_and_read_values() {
        // Verify that new object is empty
        SegmentStats stats = props.getSegmentStats();
        assertEquals(0, stats.getNumberOfKeys());
        assertEquals(0, stats.getNumberOfKeysInDeltaCache());
        assertEquals(0, stats.getNumberOfKeysInSegment());
        assertEquals(0, stats.getNumberOfKeysInScarceIndex());

        assertEquals(0, props.getCacheDeltaFileNames().size());

        // verify that first file is correct
        assertEquals("segment-00027-delta-000.cache",
                props.getAndIncreaseDeltaFileName());
        assertEquals(1, props.getCacheDeltaFileNames().size());
        assertTrue(props.getCacheDeltaFileNames()
                .contains("segment-00027-delta-000.cache"));

        // Set correct values
        props.setNumberOfKeysInCache(87);
        props.setNumberOfKeysInScarceIndex(132);
        props.setNumberOfKeysInIndex(1023);

        // verify that data are correctly read
        stats = props.getSegmentStats();
        assertEquals(1110, stats.getNumberOfKeys());
        assertEquals(87, stats.getNumberOfKeysInDeltaCache());
        assertEquals(1023, stats.getNumberOfKeysInSegment());
        assertEquals(132, stats.getNumberOfKeysInScarceIndex());

        // verify that newly added
        assertEquals("segment-00027-delta-001.cache",
                props.getAndIncreaseDeltaFileName());
        assertEquals(2, props.getCacheDeltaFileNames().size());
        assertTrue(props.getCacheDeltaFileNames()
                .contains("segment-00027-delta-000.cache"));
        assertTrue(props.getCacheDeltaFileNames()
                .contains("segment-00027-delta-001.cache"));

        props.clearCacheDeltaFileNamesCouter();
        assertEquals(0, props.getCacheDeltaFileNames().size());
    }

    @Test
    void test_deltaFileNames_are_sorted() {
        assertEquals("segment-00027-delta-000.cache",
                props.getAndIncreaseDeltaFileName());
        assertEquals("segment-00027-delta-001.cache",
                props.getAndIncreaseDeltaFileName());
        assertEquals("segment-00027-delta-002.cache",
                props.getAndIncreaseDeltaFileName());
        assertEquals("segment-00027-delta-003.cache",
                props.getAndIncreaseDeltaFileName());

        assertEquals(4, props.getCacheDeltaFileNames().size());
        assertEquals("segment-00027-delta-000.cache",
                props.getCacheDeltaFileNames().get(0));
        assertEquals("segment-00027-delta-001.cache",
                props.getCacheDeltaFileNames().get(1));
        assertEquals("segment-00027-delta-002.cache",
                props.getCacheDeltaFileNames().get(2));
        assertEquals("segment-00027-delta-003.cache",
                props.getCacheDeltaFileNames().get(3));
    }

    @Test
    void test_increase_numberOfKeysInCache() {
        assertEquals(0, props.getNumberOfKeysInDeltaCache());

        // verify increment by one
        props.incrementNumberOfKeysInCache();
        assertEquals(1, props.getNumberOfKeysInDeltaCache());

        // verify increment by 7
        props.increaseNumberOfKeysInDeltaCache(7);
        assertEquals(8, props.getNumberOfKeysInDeltaCache());

        // Verify that negative value is not allowed
        assertThrows(IllegalArgumentException.class,
                () -> props.increaseNumberOfKeysInDeltaCache(-2));

        assertEquals(8, props.getNumberOfKeysInDeltaCache());
    }

    @BeforeEach
    void beforeEeachTest() {
        directory = new MemDirectory();
        props = new SegmentPropertiesManager(directory, id);
    }

}
