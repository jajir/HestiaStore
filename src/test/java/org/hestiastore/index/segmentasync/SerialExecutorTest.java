package org.hestiastore.index.segmentasync;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class SerialExecutorTest {

    @Test
    void execute_runsTasksInOrder() {
        final Executor delegate = Runnable::run;
        final SerialExecutor executor = new SerialExecutor(delegate);
        final List<Integer> order = new ArrayList<>();

        executor.execute(() -> order.add(1));
        executor.execute(() -> order.add(2));

        assertEquals(List.of(1, 2), order);
    }

    @Test
    void execute_dropsQueuedTasksOnRejection() {
        final AtomicInteger calls = new AtomicInteger();
        final Executor delegate = command -> {
            if (calls.getAndIncrement() == 0) {
                throw new RejectedExecutionException("reject");
            }
            command.run();
        };
        final SerialExecutor executor = new SerialExecutor(delegate);
        final AtomicInteger ran = new AtomicInteger();

        executor.execute(ran::incrementAndGet);
        executor.execute(ran::incrementAndGet);

        assertEquals(1, ran.get());
    }
}
