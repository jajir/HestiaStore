package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
        assertEquals("v01-delta-0000.cache",
                reserveNextDeltaFileName());
        assertEquals(1, props.getCacheDeltaFileNames().size());
        assertTrue(props.getCacheDeltaFileNames()
                .contains("v01-delta-0000.cache"));

        // Set correct values
        props.startTx().setNumberOfKeysInCache(87)
                .setNumberOfKeysInScarceIndex(132)
                .setNumberOfKeysInIndex(1023).commit();

        // verify that data are correctly read
        stats = props.getSegmentStats();
        assertEquals(1110, stats.getNumberOfKeys());
        assertEquals(87, stats.getNumberOfKeysInDeltaCache());
        assertEquals(1023, stats.getNumberOfKeysInSegment());
        assertEquals(132, stats.getNumberOfKeysInScarceIndex());

        // verify that newly added
        assertEquals("v01-delta-0001.cache",
                reserveNextDeltaFileName());
        assertEquals(2, props.getCacheDeltaFileNames().size());
        assertTrue(props.getCacheDeltaFileNames()
                .contains("v01-delta-0000.cache"));
        assertTrue(props.getCacheDeltaFileNames()
                .contains("v01-delta-0001.cache"));

        props.clearCacheDeltaFileNamesCouter();
        assertEquals(0, props.getCacheDeltaFileNames().size());
    }

    @Test
    void test_deltaFileNames_are_sorted() {
        assertEquals("v01-delta-0000.cache",
                reserveNextDeltaFileName());
        assertEquals("v01-delta-0001.cache",
                reserveNextDeltaFileName());
        assertEquals("v01-delta-0002.cache",
                reserveNextDeltaFileName());
        assertEquals("v01-delta-0003.cache",
                reserveNextDeltaFileName());

        assertEquals(4, props.getCacheDeltaFileNames().size());
        assertEquals("v01-delta-0000.cache",
                props.getCacheDeltaFileNames().get(0));
        assertEquals("v01-delta-0001.cache",
                props.getCacheDeltaFileNames().get(1));
        assertEquals("v01-delta-0002.cache",
                props.getCacheDeltaFileNames().get(2));
        assertEquals("v01-delta-0003.cache",
                props.getCacheDeltaFileNames().get(3));
    }

    @Test
    void test_deltaFileNames_support_more_than_three_digits() {
        for (int i = 0; i < 1000; i++) {
            props.incrementDeltaFileNameCounter();
        }

        assertEquals("v01-delta-1000.cache",
                props.getNextDeltaFileName());
    }

    @Test
    void test_increase_numberOfKeysInCache() {
        assertEquals(0, props.getNumberOfKeysInDeltaCache());

        // verify increment by one
        props.incrementNumberOfKeysInCache();
        assertEquals(1, props.getNumberOfKeysInDeltaCache());

        // verify subsequent increments
        for (int i = 0; i < 7; i++) {
            props.incrementNumberOfKeysInCache();
        }
        assertEquals(8, props.getNumberOfKeysInDeltaCache());
    }

    @Test
    void deltaFileCounter_increments_under_concurrency() throws Exception {
        final int threads = 8;
        final int perThread = 50;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            executor.execute(() -> {
                try {
                    start.await();
                    for (int j = 0; j < perThread; j++) {
                        props.incrementDeltaFileNameCounter();
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS));
        executor.shutdownNow();

        assertEquals(threads * perThread, props.getDeltaFileCount());
        assertEquals("v01-delta-0400.cache", props.getNextDeltaFileName());
    }

    @Test
    void incrementNumberOfKeysInCache_is_atomic_under_concurrency()
            throws Exception {
        final int threads = 6;
        final int perThread = 100;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            executor.execute(() -> {
                try {
                    start.await();
                    for (int j = 0; j < perThread; j++) {
                        props.incrementNumberOfKeysInCache();
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS));
        executor.shutdownNow();

        assertEquals(threads * perThread, props.getNumberOfKeysInDeltaCache());
    }

    @Test
    void version_round_trip() {
        assertEquals(0L, props.getVersion());
        props.startTx().setVersion(3L).commit();
        assertEquals(3L, props.getVersion());
    }

    @Test
    void switchDirectory_resets_to_new_store() {
        props.startTx().setVersion(5L).commit();
        final Directory newDirectory = new MemDirectory();

        props.switchDirectory(newDirectory);

        assertEquals(0L, props.getVersion());
    }

    @BeforeEach
    void beforeEeachTest() {
        directory = new MemDirectory();
        props = new SegmentPropertiesManager(
                directory,
                id);
    }

    private String reserveNextDeltaFileName() {
        final String next = props.getNextDeltaFileName();
        props.startTx()
                .setDeltaFileCount(props.getDeltaFileCount() + 1)
                .commit();
        return next;
    }

}
