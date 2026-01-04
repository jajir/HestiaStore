package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segmentasync.SegmentAsync;
import org.hestiastore.index.segmentasync.SegmentAsyncAdapter;
import org.hestiastore.index.segmentasync.SegmentMaintenancePolicy;
import org.hestiastore.index.segmentasync.SegmentMaintenancePolicyThreshold;
import org.junit.jupiter.api.Test;

class SegmentAsyncAdapterTest {

    @Test
    void flushAndCompactAreSerializedPerSegment() throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final AtomicInteger active = new AtomicInteger();
            final AtomicInteger maxActive = new AtomicInteger();
            final CountDownLatch flushStarted = new CountDownLatch(1);
            final CountDownLatch allowFlushFinish = new CountDownLatch(1);

            final TestSegment segment = new TestSegment(SegmentId.of(1), () -> {
                final int current = active.incrementAndGet();
                maxActive.updateAndGet(prev -> Math.max(prev, current));
                flushStarted.countDown();
                await(allowFlushFinish);
                active.decrementAndGet();
            }, () -> {
                final int current = active.incrementAndGet();
                maxActive.updateAndGet(prev -> Math.max(prev, current));
                active.decrementAndGet();
            });

            final SegmentAsync<Integer, Integer> async = new SegmentAsyncAdapter<>(
                    segment, executor, SegmentMaintenancePolicy.none());
            final CompletionStage<Void> flushFuture = async.flushAsync();
            assertTrue(flushStarted.await(1, TimeUnit.SECONDS));
            final CompletionStage<Void> compactFuture = async.compactAsync();
            allowFlushFinish.countDown();

            flushFuture.toCompletableFuture().get(1, TimeUnit.SECONDS);
            compactFuture.toCompletableFuture().get(1, TimeUnit.SECONDS);
            assertEquals(1, maxActive.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void maintenanceCanRunConcurrentlyAcrossSegments() throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final CountDownLatch started = new CountDownLatch(2);
            final CountDownLatch allowFinish = new CountDownLatch(1);

            final TestSegment segmentOne = new TestSegment(SegmentId.of(1),
                    () -> {
                        started.countDown();
                        await(allowFinish);
                    }, () -> {
                    });
            final TestSegment segmentTwo = new TestSegment(SegmentId.of(2),
                    () -> {
                        started.countDown();
                        await(allowFinish);
                    }, () -> {
                    });

            final SegmentAsync<Integer, Integer> asyncOne = new SegmentAsyncAdapter<>(
                    segmentOne, executor, SegmentMaintenancePolicy.none());
            final SegmentAsync<Integer, Integer> asyncTwo = new SegmentAsyncAdapter<>(
                    segmentTwo, executor, SegmentMaintenancePolicy.none());

            final CompletionStage<Void> flushOne = asyncOne.flushAsync();
            final CompletionStage<Void> flushTwo = asyncTwo.flushAsync();

            assertTrue(started.await(1, TimeUnit.SECONDS));
            allowFinish.countDown();

            flushOne.toCompletableFuture().get(1, TimeUnit.SECONDS);
            flushTwo.toCompletableFuture().get(1, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void autoFlushTriggersOnWriteCacheThreshold() throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(1);
        try {
            final AtomicInteger writeCache = new AtomicInteger();
            final CountDownLatch flushCalled = new CountDownLatch(1);

            final TestSegment segment = new TestSegment(SegmentId.of(3), () -> {
                flushCalled.countDown();
            }, () -> {
            }, writeCache::incrementAndGet, writeCache::get);

            final SegmentAsync<Integer, Integer> async = new SegmentAsyncAdapter<>(
                    segment, executor,
                    new SegmentMaintenancePolicyThreshold<>(2));

            async.put(1, 1);
            assertEquals(1, writeCache.get());
            assertEquals(1, flushCalled.getCount());

            async.put(2, 2);
            assertTrue(flushCalled.await(1, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    private static void await(final CountDownLatch latch) {
        try {
            latch.await(2, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Latch interrupted", e);
        }
    }

    private static final class TestSegment extends AbstractCloseableResource
            implements Segment<Integer, Integer> {

        private final SegmentId id;
        private final Runnable onFlush;
        private final Runnable onCompact;
        private final Runnable onPut;
        private final IntSupplier writeCacheSize;

        private TestSegment(final SegmentId id, final Runnable onFlush,
                final Runnable onCompact) {
            this(id, onFlush, onCompact, null, () -> 0);
        }

        private TestSegment(final SegmentId id, final Runnable onFlush,
                final Runnable onCompact, final Runnable onPut,
                final IntSupplier writeCacheSize) {
            this.id = id;
            this.onFlush = onFlush;
            this.onCompact = onCompact;
            this.onPut = onPut;
            this.writeCacheSize = writeCacheSize;
        }

        @Override
        public SegmentStats getStats() {
            return new SegmentStats(0, 0, 0);
        }

        @Override
        public void compact() {
            onCompact.run();
        }

        @Override
        public Integer checkAndRepairConsistency() {
            return null;
        }

        @Override
        public void invalidateIterators() {
        }

        @Override
        public EntryIterator<Integer, Integer> openIterator(
                final SegmentIteratorIsolation isolation) {
            return EntryIterator.make(Collections.emptyIterator());
        }

        @Override
        public void put(final Integer key, final Integer value) {
            if (onPut != null) {
                onPut.run();
            }
        }

        @Override
        public void flush() {
            onFlush.run();
        }

        @Override
        public int getNumberOfKeysInWriteCache() {
            return writeCacheSize.getAsInt();
        }

        @Override
        public long getNumberOfKeysInCache() {
            return 0;
        }

        @Override
        public long getNumberOfKeys() {
            return 0;
        }

        @Override
        public Integer get(final Integer key) {
            return null;
        }

        @Override
        public SegmentId getId() {
            return id;
        }

        @Override
        protected void doClose() {
        }
    }
}
