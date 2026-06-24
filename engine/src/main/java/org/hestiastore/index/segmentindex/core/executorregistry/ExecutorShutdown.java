package org.hestiastore.index.segmentindex.core.executorregistry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.IndexException;

/**
 * Shared shutdown helper for executor owners that aggregate close failures.
 */
final class ExecutorShutdown {

    private ExecutorShutdown() {
    }

    /**
     * Shuts down one executor and appends timeout/interruption failures to the
     * current failure chain.
     *
     * @param executorName executor name used in diagnostics
     * @param executor executor service to close
     * @param shutdownTimeoutMillis await timeout in milliseconds
     * @param failure current failure, or {@code null}
     * @return current or appended failure
     */
    static RuntimeException shutdownAndAwait(final String executorName,
            final ExecutorService executor,
            final int shutdownTimeoutMillis,
            final RuntimeException failure) {
        RuntimeException nextFailure = failure;
        executor.shutdown();
        boolean interrupted = false;
        try {
            if (!executor.awaitTermination(shutdownTimeoutMillis,
                    TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
                nextFailure = appendFailure(nextFailure, new IndexException(
                        String.format(
                                "Executor '%s' did not terminate within %d ms.",
                                executorName, shutdownTimeoutMillis)));
            }
        } catch (final InterruptedException e) {
            interrupted = true;
            executor.shutdownNow();
            nextFailure = appendFailure(nextFailure, new IndexException(
                    String.format("Executor '%s' shutdown was interrupted.",
                            executorName),
                    e));
        } catch (final RuntimeException e) {
            nextFailure = appendFailure(nextFailure, e);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        return nextFailure;
    }

    private static RuntimeException appendFailure(final RuntimeException failure,
            final RuntimeException nextFailure) {
        if (failure == null) {
            return nextFailure;
        }
        failure.addSuppressed(nextFailure);
        return failure;
    }
}
