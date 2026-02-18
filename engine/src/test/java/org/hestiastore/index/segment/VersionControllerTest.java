package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class VersionControllerTest {

    @Test
    void changeVersion_increments_sequentially() {
        final VersionController controller = new VersionController();
        assertEquals(0, controller.getVersion());
        controller.changeVersion();
        controller.changeVersion();
        assertEquals(2, controller.getVersion());
    }

    @Test
    void changeVersion_is_thread_safe() throws Exception {
        final VersionController controller = new VersionController();
        final int threads = 4;
        final int perThread = 500;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch ready = new CountDownLatch(threads);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);

        try {
            for (int i = 0; i < threads; i++) {
                executor.execute(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        for (int j = 0; j < perThread; j++) {
                            controller.changeVersion();
                        }
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }

            assertTrue(ready.await(5, TimeUnit.SECONDS),
                    "Workers did not start in time");
            start.countDown();
            assertTrue(done.await(5, TimeUnit.SECONDS),
                    "Workers did not finish in time");
        } finally {
            executor.shutdownNow();
        }

        assertEquals(threads * perThread, controller.getVersion());
    }

    @Test
    void changeVersion_throws_when_max_reached() throws Exception {
        final VersionController controller = new VersionController();
        final Field field = VersionController.class
                .getDeclaredField("segmentVersion");
        field.setAccessible(true);
        final AtomicInteger counter = (AtomicInteger) field.get(controller);
        counter.set(Integer.MAX_VALUE);

        assertThrows(IllegalStateException.class, controller::changeVersion);
        assertEquals(Integer.MAX_VALUE, controller.getVersion());
    }
}
