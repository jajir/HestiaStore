package org.coroptis.index.it;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.datatype.ConvertorFromBytes;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.junit.jupiter.api.Test;

/**
 * Integration tests that ensure {@link SegmentIndex#close()} waits for
 * in-flight {@code putAsync/getAsync/deleteAsync} operations before releasing
 * resources.
 *
 * <p>
 * Expected behavior (contract these tests assert): once {@code close()} is
 * invoked it should not return while async operations are still in progress,
 * because the index releases its directory lock and closes internal resources.
 * </p>
 */
class SegmentIndexAsyncCloseRaceIT {

    /**
     * TypeDescriptor that lets tests deterministically block inside
     * {@link TypeDescriptor#getTombstone()} while an index operation is holding
     * internal state.
     */
    public static final class BlockingTombstoneTypeDescriptorString
            implements TypeDescriptor<String> {

        static final class Hook {
            final CountDownLatch entered = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
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

        private final TypeDescriptorString delegate = new TypeDescriptorString();

        public BlockingTombstoneTypeDescriptorString() {
        }

        @Override
        public Comparator<String> getComparator() {
            return delegate.getComparator();
        }

        @Override
        public TypeReader<String> getTypeReader() {
            return delegate.getTypeReader();
        }

        @Override
        public TypeWriter<String> getTypeWriter() {
            return delegate.getTypeWriter();
        }

        @Override
        public ConvertorFromBytes<String> getConvertorFromBytes() {
            return delegate.getConvertorFromBytes();
        }

        @Override
        public ConvertorToBytes<String> getConvertorToBytes() {
            return delegate.getConvertorToBytes();
        }

        /**
         * Mainlyit return timbstone value. That it signalizae that it was
         * entered and waits for test to release it. Wait timeout is 10 seconds
         * to avoid deadlocks in tests.
         */
        @Override
        public String getTombstone() {
            final Hook hook = HOOK.get();
            if (hook != null) {
                hook.entered.countDown();
                try {
                    if (!hook.release.await(10, TimeUnit.SECONDS)) {
                        throw new TimeoutException(
                                "Timed out waiting for test to release tombstone hook.");
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "Interrupted while waiting on tombstone hook", e);
                } catch (final TimeoutException e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
            }
            return delegate.getTombstone();
        }
    }

    private IndexConfiguration<String, String> conf() {
        return IndexConfiguration.<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withName("async-close-race")//
                .withKeyTypeDescriptor(new TypeDescriptorString())//
                .withValueTypeDescriptor(
                        new BlockingTombstoneTypeDescriptorString())//
                .withMaxNumberOfKeysInCache(10_000)//
                .build();
    }

    @Test
    void close_waits_for_inflight_putAsync() throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<String, String> conf = conf();

        final SegmentIndex<String, String> index = SegmentIndex
                .create(directory, conf);
        final BlockingTombstoneTypeDescriptorString.Hook hook = BlockingTombstoneTypeDescriptorString
                .installHook();
        final ExecutorService closeExecutor = Executors
                .newSingleThreadExecutor();
        try {
            final CompletionStage<Void> putStage = index.putAsync("k", "v");
            final CompletableFuture<Void> putFuture = putStage
                    .toCompletableFuture();

            // Make sure that getTombstone() is waiting
            assertTrue(hook.entered.await(5, TimeUnit.SECONDS),
                    "putAsync did not reach tombstone hook in time");

            final CompletableFuture<Void> closeFuture = CompletableFuture
                    .runAsync(index::close, closeExecutor);

            assertThrows(TimeoutException.class,
                    () -> closeFuture.get(5, TimeUnit.SECONDS),
                    "close() returned while putAsync was still in-flight");

            hook.release.countDown();
            putFuture.get(5, TimeUnit.SECONDS);
            closeFuture.get(5, TimeUnit.SECONDS);
        } finally {
            hook.release.countDown();
            BlockingTombstoneTypeDescriptorString.clearHook();
            closeExecutor.shutdownNow();
            try {
                index.close();
            } catch (final Throwable ignore) {
                // best-effort cleanup for failing assertions
            }
        }
    }

    @Test
    void close_waits_for_inflight_deleteAsync() throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<String, String> conf = conf();

        final SegmentIndex<String, String> index = SegmentIndex
                .create(directory, conf);
        index.put("k", "v");

        final BlockingTombstoneTypeDescriptorString.Hook hook = BlockingTombstoneTypeDescriptorString
                .installHook();
        final ExecutorService closeExecutor = Executors
                .newSingleThreadExecutor();
        try {
            final CompletableFuture<Void> deleteFuture = index.deleteAsync("k")
                    .toCompletableFuture();

            assertTrue(hook.entered.await(5, TimeUnit.SECONDS),
                    "deleteAsync did not reach tombstone hook in time");

            final CompletableFuture<Void> closeFuture = CompletableFuture
                    .runAsync(index::close, closeExecutor);

            assertThrows(TimeoutException.class,
                    () -> closeFuture.get(2, TimeUnit.SECONDS),
                    "close() returned while deleteAsync was still in-flight");

            hook.release.countDown();
            deleteFuture.get(5, TimeUnit.SECONDS);
            closeFuture.get(5, TimeUnit.SECONDS);
        } finally {
            hook.release.countDown();
            BlockingTombstoneTypeDescriptorString.clearHook();
            closeExecutor.shutdownNow();
            try {
                index.close();
            } catch (final Throwable ignore) {
                // best-effort cleanup for failing assertions
            }
        }
    }

    @Test
    void close_waits_for_inflight_getAsync() throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<String, String> conf = conf();

        final SegmentIndex<String, String> index = SegmentIndex
                .create(directory, conf);
        index.put("k", "v");

        final BlockingTombstoneTypeDescriptorString.Hook hook = BlockingTombstoneTypeDescriptorString
                .installHook();
        final ExecutorService closeExecutor = Executors
                .newSingleThreadExecutor();
        try {
            final CompletableFuture<String> getFuture = index.getAsync("k")
                    .toCompletableFuture();

            assertTrue(hook.entered.await(5, TimeUnit.SECONDS),
                    "getAsync did not reach tombstone hook in time");

            final CompletableFuture<Void> closeFuture = CompletableFuture
                    .runAsync(index::close, closeExecutor);

            assertThrows(TimeoutException.class,
                    () -> closeFuture.get(2, TimeUnit.SECONDS),
                    "close() returned while getAsync was still in-flight");

            hook.release.countDown();
            assertEquals("v", getFuture.get(5, TimeUnit.SECONDS));
            closeFuture.get(5, TimeUnit.SECONDS);
        } finally {
            hook.release.countDown();
            BlockingTombstoneTypeDescriptorString.clearHook();
            closeExecutor.shutdownNow();
            try {
                index.close();
            } catch (final Throwable ignore) {
                // best-effort cleanup for failing assertions
            }
        }
    }
}
