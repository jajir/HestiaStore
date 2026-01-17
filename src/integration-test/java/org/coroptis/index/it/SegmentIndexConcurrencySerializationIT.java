package org.coroptis.index.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.junit.jupiter.api.Test;

class SegmentIndexConcurrencySerializationIT {

    private static final String TEST_KEY = "k";
    private static final String TEST_VALUE = "v";
    private static final int HOOK_WAIT_SECONDS = 5;
    private static final int RESULT_WAIT_SECONDS = 5;

    private enum ReadMode {
        SYNC, ASYNC
    }

    /**
     * 
     * Test verify that gets really overlaps.
     * 
     * 1) Test put new key value pait into index
     * 
     * 2) Test start two get(k) in parallel threads. Execution of both gets
     * should overlap
     * 
     * 
     * @throws Exception
     */
    @Test
    void parallelReadsOverlap_with_two_threads() throws Exception {
        runReadOverlapTest(ReadMode.SYNC, "read-lock-overlap");
    }

    @Test
    void parallelAsyncReadsOverlap_with_two_threads() throws Exception {
        runReadOverlapTest(ReadMode.ASYNC, "read-lock-async-overlap");
    }

    private void runReadOverlapTest(final ReadMode mode, final String name)
            throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<String, String> conf = readLockConf(name);
        BlockingTombstoneTypeDescriptorString.Hook hook = null;
        final ExecutorService executor = mode == ReadMode.SYNC
                ? Executors.newFixedThreadPool(2)
                : null;
        try (SegmentIndex<String, String> index = SegmentIndex
                .create(directory, conf)) {
            index.put(TEST_KEY, TEST_VALUE);
            hook = BlockingTombstoneTypeDescriptorString.installHook();
            final CompletableFuture<String> first = startRead(mode, index,
                    executor);
            assertTrue(
                    hook.firstEntered.await(HOOK_WAIT_SECONDS,
                            TimeUnit.SECONDS),
                    "First get() is waiting for tombstone hook");

            final CompletableFuture<String> second = startRead(mode, index,
                    executor);
            assertTrue(
                    hook.secondEntered.await(HOOK_WAIT_SECONDS,
                            TimeUnit.SECONDS),
                    "Second get() is waiting for tombstone hook");
            assertTrue(hook.overlapDetected.get(),
                    "Expected overlapping reads");

            assertFalse(first.isDone(),
                    "First get() returned before being released");
            assertFalse(second.isDone(),
                    "Second get() returned before being released");

            hook.release.countDown();

            assertEquals(TEST_VALUE,
                    first.get(RESULT_WAIT_SECONDS, TimeUnit.SECONDS));
            assertEquals(TEST_VALUE,
                    second.get(RESULT_WAIT_SECONDS, TimeUnit.SECONDS));
            assertTrue(first.isDone(),
                    "First get() is still not done after being released");
            assertTrue(second.isDone(),
                    "Second get() is still not done after being released");
        } finally {
            if (hook != null) {
                hook.release.countDown();
            }
            BlockingTombstoneTypeDescriptorString.clearHook();
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }

    private CompletableFuture<String> startRead(final ReadMode mode,
            final SegmentIndex<String, String> index,
            final ExecutorService executor) {
        if (mode == ReadMode.ASYNC) {
            return index.getAsync(TEST_KEY).toCompletableFuture();
        }
        return CompletableFuture.supplyAsync(() -> index.get(TEST_KEY),
                executor);
    }

    private static IndexConfiguration<String, String> readLockConf(
            final String name) {
        final IndexConfigurationContract defaults = new IndexConfigurationContract() {
        };
        return IndexConfiguration.<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorString())//
                .withValueTypeDescriptor(
                        new BlockingTombstoneTypeDescriptorString())//
                .withName(name)//
                .withContextLoggingEnabled(false)//
                .withMaxNumberOfKeysInCache(10_000)//
                .withMaxNumberOfKeysInSegment(
                        defaults.getMaxNumberOfKeysInSegment())//
                .withMaxNumberOfSegmentsInCache(
                        defaults.getMaxNumberOfSegmentsInCache())//
                .withMaxNumberOfKeysInSegmentCache(
                        defaults.getMaxNumberOfKeysInSegmentCache())//
                .withMaxNumberOfKeysInSegmentWriteCache(
                        defaults.getMaxNumberOfKeysInSegmentWriteCache())//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(
                        defaults.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance())//
                .withMaxNumberOfKeysInSegmentChunk(
                        defaults.getMaxNumberOfKeysInSegmentChunk())//
                .withBloomFilterNumberOfHashFunctions(
                        defaults.getBloomFilterNumberOfHashFunctions())//
                .withBloomFilterIndexSizeInBytes(
                        defaults.getBloomFilterIndexSizeInBytes())//
                .withBloomFilterProbabilityOfFalsePositive(
                        defaults.getBloomFilterProbabilityOfFalsePositive())//
                .withDiskIoBufferSizeInBytes(
                        defaults.getDiskIoBufferSizeInBytes())//
                .withNumberOfIoThreads(defaults.getNumberOfIoThreads())//
                .withNumberOfSegmentIndexMaintenanceThreads(
                        defaults.getNumberOfSegmentIndexMaintenanceThreads())//
                .withIndexBusyBackoffMillis(
                        defaults.getIndexBusyBackoffMillis())//
                .withIndexBusyTimeoutMillis(
                        defaults.getIndexBusyTimeoutMillis())//
                .withSegmentMaintenanceAutoEnabled(
                        defaults.isSegmentMaintenanceAutoEnabled())//
                .withEncodingFilters(defaults.getEncodingChunkFilters())//
                .withDecodingFilters(defaults.getDecodingChunkFilters())//
                .build();
    }

    public static final class BlockingTombstoneTypeDescriptorString
            extends AbstractBlockingTombstoneTypeDescriptorString {

        static final class Hook {
            /**
             * Latchec help verify that code passed method getTombstone in both
             * calls.
             */
            final CountDownLatch firstEntered = new CountDownLatch(1);
            final CountDownLatch secondEntered = new CountDownLatch(1);

            /**
             * Allows to test to unlock getTombstone() method for both threads
             */
            final CountDownLatch release = new CountDownLatch(1);

            /**
             * Number of calls currently in getTombstone()
             */
            final AtomicInteger inCall = new AtomicInteger(0);

            final AtomicInteger totalEntered = new AtomicInteger(0);
            /**
             * Set to true when overlapping calls are detected
             */
            final AtomicBoolean overlapDetected = new AtomicBoolean(false);
        }

        private static final AtomicReference<Hook> HOOK = new AtomicReference<>();

        static Hook installHook() {
            final Hook hook = new Hook();
            HOOK.set(hook);
            return hook;
        }

        static void clearHook() {
            HOOK.set(null);
        }

        @Override
        public String getTombstone() {
            final Hook hook = HOOK.get();
            if (hook != null) {
                final int concurrent = hook.inCall.incrementAndGet();
                if (concurrent > 1) {
                    hook.overlapDetected.set(true);
                }
                final int total = hook.totalEntered.incrementAndGet();
                if (total == 1) {
                    hook.firstEntered.countDown();
                } else if (total == 2) {
                    hook.secondEntered.countDown();
                }
                try {
                    if (!hook.release.await(6, TimeUnit.SECONDS)) {
                        throw new TimeoutException(
                                "Timed out waiting for read overlap hook.");
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "Interrupted while waiting on read overlap hook",
                            e);
                } catch (final TimeoutException e) {
                    throw new IllegalStateException(e.getMessage(), e);
                } finally {
                    hook.inCall.decrementAndGet();
                }
            }
            return delegate.getTombstone();
        }
    }
}
