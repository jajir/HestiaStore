package org.hestiastore.index.segmentasync;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentImplSynchronizationAdapter;

/**
 * Async-capable segment that serializes maintenance tasks on a shared executor.
 */
public final class SegmentAsyncAdapter<K, V>
        extends SegmentImplSynchronizationAdapter<K, V>
        implements SegmentAsync<K, V> {

    private final SerialExecutor maintenanceExecutor;

    public SegmentAsyncAdapter(final Segment<K, V> delegate,
            final Executor sharedExecutor) {
        super(delegate);
        this.maintenanceExecutor = new SerialExecutor(sharedExecutor);
    }

    @Override
    public CompletionStage<Void> flushAsync() {
        return submit(super::flush);
    }

    @Override
    public CompletionStage<Void> compactAsync() {
        return submit(super::compact);
    }

    private CompletionStage<Void> submit(final Runnable task) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        maintenanceExecutor.execute(() -> {
            try {
                task.run();
                future.complete(null);
            } catch (final Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }
}
