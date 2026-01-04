package org.hestiastore.index.segmentasync;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentImplSynchronizationAdapter;

/**
 * Async-capable segment that serializes maintenance tasks on a shared executor.
 */
public final class SegmentAsyncAdapter<K, V>
        extends SegmentImplSynchronizationAdapter<K, V>
        implements SegmentAsync<K, V> {

    private final SerialExecutor maintenanceExecutor;
    private final SegmentMaintenancePolicy<K, V> maintenancePolicy;

    public SegmentAsyncAdapter(final Segment<K, V> delegate,
            final Executor sharedExecutor,
            final SegmentMaintenancePolicy<K, V> maintenancePolicy) {
        super(delegate);
        this.maintenanceExecutor = new SerialExecutor(sharedExecutor);
        this.maintenancePolicy = Vldtn.requireNonNull(maintenancePolicy,
                "maintenancePolicy");
    }

    @Override
    public void put(final K key, final V value) {
        super.put(key, value);
        scheduleMaintenanceIfNeeded();
    }

    @Override
    public void flush() {
        flushAsync();
    }

    @Override
    public void compact() {
        compactAsync();
    }

    @Override
    public CompletionStage<Void> flushAsync() {
        return submit(super::flush);
    }

    @Override
    public CompletionStage<Void> compactAsync() {
        return submit(super::compact);
    }

    private void scheduleMaintenanceIfNeeded() {
        final SegmentMaintenanceDecision decision = maintenancePolicy
                .evaluateAfterWrite(this);
        if (decision.shouldFlush()) {
            flushAsync();
        }
        if (decision.shouldCompact()) {
            compactAsync();
        }
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
