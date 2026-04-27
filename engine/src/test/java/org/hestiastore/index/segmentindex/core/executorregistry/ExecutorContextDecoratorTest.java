package org.hestiastore.index.segmentindex.core.executorregistry;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

class ExecutorContextDecoratorTest {

    @Test
    void decorateReturnsOriginalExecutorWhenContextLoggingIsDisabled() {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final ExecutorService decorated = new ExecutorContextDecorator(
                    false, "executor-context-decorator-test")
                            .decorate(executor);

            assertSame(executor, decorated);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void decorateWrapsExecutorWhenContextLoggingIsEnabled() {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final ExecutorService decorated = new ExecutorContextDecorator(
                    true, "executor-context-decorator-test")
                            .decorate(executor);

            assertInstanceOf(IndexNameMdcExecutorService.class, decorated);
        } finally {
            executor.shutdownNow();
        }
    }
}
