package org.hestiastore.index.segmentasync;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executor;

/**
 * Serializes tasks on top of a shared executor.
 */
final class SerialExecutor implements Executor {

    private final Queue<Runnable> tasks = new ArrayDeque<>();
    private final Executor executor;
    private Runnable active;

    SerialExecutor(final Executor executor) {
        this.executor = executor;
    }

    @Override
    public synchronized void execute(final Runnable command) {
        tasks.offer(() -> {
            try {
                command.run();
            } finally {
                scheduleNext();
            }
        });
        if (active == null) {
            scheduleNext();
        }
    }

    private synchronized void scheduleNext() {
        if ((active = tasks.poll()) != null) {
            executor.execute(active);
        }
    }
}
