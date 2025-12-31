package org.hestiastore.index.segment;

import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.OptimisticLockObjectVersionProvider;

/**
 * Holds information about segment version.
 * 
 * Allows to create optimistic lock.
 * 
 * @author honza
 *
 */
public class VersionController implements OptimisticLockObjectVersionProvider {

    private final AtomicInteger segmentVersion = new AtomicInteger(0);

    public void changeVersion() {
        while (true) {
            final int current = segmentVersion.get();
            if (current == Integer.MAX_VALUE) {
                throw new IllegalStateException(
                        "Segment version reached maximum value");
            }
            final int next = current + 1;
            if (segmentVersion.compareAndSet(current, next)) {
                return;
            }
        }
    }

    @Override
    public int getVersion() {
        return segmentVersion.get();
    }

}
