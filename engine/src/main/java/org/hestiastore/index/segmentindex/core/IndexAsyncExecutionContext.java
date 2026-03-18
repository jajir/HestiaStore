package org.hestiastore.index.segmentindex.core;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;

/**
 * Marks threads currently executing index work through async API executors.
 */
final class IndexAsyncExecutionContext {

    private static final ThreadLocal<Integer> asyncDepth = ThreadLocal
            .withInitial(() -> Integer.valueOf(0));

    private IndexAsyncExecutionContext() {
    }

    static <T> T runInAsyncContext(final Supplier<T> task) {
        final Supplier<T> nonNullTask = Vldtn.requireNonNull(task, "task");
        final int previousDepth = asyncDepth.get().intValue();
        asyncDepth.set(Integer.valueOf(previousDepth + 1));
        try {
            return nonNullTask.get();
        } finally {
            asyncDepth.set(Integer.valueOf(previousDepth));
        }
    }

    static boolean isAsyncOperationActive() {
        return asyncDepth.get().intValue() > 0;
    }
}
