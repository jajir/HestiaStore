package org.hestiastore.index.senku.internal;

import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Cancellation token for a periodic scan advanced explicitly by a test. */
final class SenkuManualScheduledFuture extends FutureTask<Void>
        implements ScheduledFuture<Void> {

    SenkuManualScheduledFuture() {
        super(() -> null);
    }

    /** {@inheritDoc} */
    @Override
    public long getDelay(final TimeUnit unit) {
        return 0L;
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(final Delayed other) {
        return Long.compare(getDelay(TimeUnit.NANOSECONDS),
                other.getDelay(TimeUnit.NANOSECONDS));
    }
}
