package org.hestiastore.index.segmentindex.core.lifecycle;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;

/**
 * Named close bridge that shuts down the lifecycle-owned startup resources
 * after the runtime index itself is closed.
 */
final class SegmentIndexLifecycleCloseResource
        extends AbstractCloseableResource {

    private final SegmentIndexLifecycle<?, ?> lifecycle;

    SegmentIndexLifecycleCloseResource(
            final SegmentIndexLifecycle<?, ?> lifecycle) {
        this.lifecycle = Vldtn.requireNonNull(lifecycle, "lifecycle");
    }

    @Override
    protected void doClose() {
        lifecycle.close();
    }
}
