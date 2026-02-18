package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

class DirectExecutorTest {

    @Test
    void execute_runs_inline_on_calling_thread() {
        final DirectExecutor executor = new DirectExecutor();
        final Thread caller = Thread.currentThread();
        final AtomicReference<Thread> executedOn = new AtomicReference<>();

        executor.execute(() -> executedOn.set(Thread.currentThread()));

        assertEquals(caller, executedOn.get());
    }
}
