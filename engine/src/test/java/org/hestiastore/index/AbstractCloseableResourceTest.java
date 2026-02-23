package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class AbstractCloseableResourceTest {

    private static final class Probe extends AbstractCloseableResource {

        @Override
        protected void doClose() {
            // no-op test stub
        }
    }

    private static final class CountingProbe extends AbstractCloseableResource {

        private final AtomicInteger closeCalls = new AtomicInteger(0);

        @Override
        protected void doClose() {
            closeCalls.incrementAndGet();
        }

        private int getCloseCalls() {
            return closeCalls.get();
        }
    }

    @Test
    void closeMarksResourceAsClosed() {
        Probe probe = new Probe();
        assertFalse(probe.wasClosed());

        probe.close();

        assertTrue(probe.wasClosed());
    }

    @Test
    void secondCloseThrows() {
        Probe probe = new Probe();
        probe.close();

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                probe::close);
        assertTrue(ex.getMessage().contains("already closed"));
    }

    @Test
    void concurrentCloseInvokesDoCloseOnlyOnce() throws Exception {
        final CountingProbe probe = new CountingProbe();
        final int concurrency = 8;
        final ExecutorService executor = Executors
                .newFixedThreadPool(concurrency);
        final CountDownLatch ready = new CountDownLatch(concurrency);
        final CountDownLatch start = new CountDownLatch(1);
        final List<Future<Boolean>> results = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            results.add(executor.submit(() -> {
                ready.countDown();
                start.await(5, TimeUnit.SECONDS);
                try {
                    probe.close();
                    return Boolean.TRUE;
                } catch (final IllegalStateException e) {
                    return Boolean.FALSE;
                }
            }));
        }
        assertTrue(ready.await(5, TimeUnit.SECONDS));
        start.countDown();

        int successfulCloseCalls = 0;
        for (final Future<Boolean> result : results) {
            if (Boolean.TRUE.equals(result.get())) {
                successfulCloseCalls++;
            }
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(1, successfulCloseCalls);
        assertEquals(1, probe.getCloseCalls());
        assertTrue(probe.wasClosed());
    }
}
