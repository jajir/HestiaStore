package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class SenkuFlushExecutorTest {
    @Test
    void pageByteBudgetIsSharedAndReusable() {
        final int budget = SenkuFlushExecutor.PAGE_BYTES_BUDGET;
        SenkuFlushExecutor.reserve(budget);
        try {
            assertFalse(SenkuFlushExecutor.tryReserve(1));
        } finally {
            SenkuFlushExecutor.release(budget);
        }
        assertTrue(SenkuFlushExecutor.tryReserve(budget));
        SenkuFlushExecutor.release(budget);
        assertTrue(SenkuFlushExecutor.parallelism() >= 1);
        assertTrue(SenkuFlushExecutor.parallelism() <= 4);
    }

    @Test
    void interruptionDoesNotConsumePermitsAndPreservesInterrupt() {
        Thread.currentThread().interrupt();
        try {
            assertThrows(IndexException.class,
                    () -> SenkuFlushExecutor.reserve(1));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        assertTrue(SenkuFlushExecutor
                .tryReserve(SenkuFlushExecutor.PAGE_BYTES_BUDGET));
        SenkuFlushExecutor.release(SenkuFlushExecutor.PAGE_BYTES_BUDGET);
    }
}
