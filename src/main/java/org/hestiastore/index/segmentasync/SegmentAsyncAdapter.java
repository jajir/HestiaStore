package org.hestiastore.index.segmentasync;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentImplSynchronizationAdapter;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;

/**
 * Async-capable segment that serializes maintenance tasks on a shared executor.
 */
public final class SegmentAsyncAdapter<K, V>
        extends SegmentImplSynchronizationAdapter<K, V>
        implements SegmentAsync<K, V> {

    private final SegmentMaintenanceScheduler<K, V> maintenanceScheduler;

    public SegmentAsyncAdapter(final Segment<K, V> delegate,
            final Executor sharedExecutor,
            final SegmentMaintenancePolicy<K, V> maintenancePolicy) {
        super(delegate);
        this.maintenanceScheduler = new SegmentMaintenanceScheduler<>(
                sharedExecutor, maintenancePolicy);
    }

    @Override
    public SegmentResult<Void> put(final K key, final V value) {
        final SegmentResult<Void> result = super.put(key, value);
        if (result.getStatus() != SegmentResultStatus.CLOSED
                && result.getStatus() != SegmentResultStatus.ERROR) {
            scheduleMaintenanceIfNeeded();
        }
        return result;
    }

    @Override
    public SegmentResult<Void> flush() {
        return flushAsync().toCompletableFuture().join();
    }

    @Override
    public SegmentResult<Void> compact() {
        return compactAsync().toCompletableFuture().join();
    }

    @Override
    public CompletionStage<SegmentResult<Void>> flushAsync() {
        return maintenanceScheduler.submit(SegmentMaintenanceTask.FLUSH,
                super::flush);
    }

    @Override
    public CompletionStage<SegmentResult<Void>> compactAsync() {
        return maintenanceScheduler.submit(SegmentMaintenanceTask.COMPACT,
                super::compact);
    }

    public CompletionStage<SegmentResult<Void>> submitMaintenanceTask(
            final SegmentMaintenanceTask taskType,
            final Runnable task) {
        return maintenanceScheduler.submit(taskType, () -> {
            task.run();
            return SegmentResult.ok();
        });
    }

    public SegmentResult<Void> flushBlocking() {
        return super.flush();
    }

    public SegmentResult<Void> compactBlocking() {
        return super.compact();
    }

    private void scheduleMaintenanceIfNeeded() {
        maintenanceScheduler.scheduleAfterWrite(this, super::flush,
                super::compact);
    }
}
