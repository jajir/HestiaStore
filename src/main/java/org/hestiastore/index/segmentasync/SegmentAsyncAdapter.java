package org.hestiastore.index.segmentasync;

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

    private final SegmentMaintenanceScheduler<K, V> maintenanceScheduler;

    public SegmentAsyncAdapter(final Segment<K, V> delegate,
            final Executor sharedExecutor,
            final SegmentMaintenancePolicy<K, V> maintenancePolicy) {
        super(delegate);
        this.maintenanceScheduler = new SegmentMaintenanceScheduler<>(
                sharedExecutor, maintenancePolicy);
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
        return maintenanceScheduler.submit(SegmentMaintenanceTask.FLUSH,
                super::flush);
    }

    @Override
    public CompletionStage<Void> compactAsync() {
        return maintenanceScheduler.submit(SegmentMaintenanceTask.COMPACT,
                super::compact);
    }

    public CompletionStage<Void> submitMaintenanceTask(
            final SegmentMaintenanceTask taskType, final Runnable task) {
        return maintenanceScheduler.submit(taskType, task);
    }

    public void flushBlocking() {
        super.flush();
    }

    public void compactBlocking() {
        super.compact();
    }

    private void scheduleMaintenanceIfNeeded() {
        maintenanceScheduler.scheduleAfterWrite(this, super::flush,
                super::compact);
    }
}
