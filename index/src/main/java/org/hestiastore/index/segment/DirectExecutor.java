package org.hestiastore.index.segment;

import java.util.concurrent.Executor;

/**
 * Executor that runs tasks immediately on the calling thread.
 */
final class DirectExecutor implements Executor {

    /**
     * Runs the command synchronously in the current thread.
     *
     * @param command runnable task to execute
     */
    @Override
    public void execute(final Runnable command) {
        command.run();
    }
}
