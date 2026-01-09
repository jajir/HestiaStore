package org.hestiastore.index.segment;

import java.util.concurrent.Executor;

final class DirectExecutor implements Executor {

    @Override
    public void execute(final Runnable command) {
        command.run();
    }
}
