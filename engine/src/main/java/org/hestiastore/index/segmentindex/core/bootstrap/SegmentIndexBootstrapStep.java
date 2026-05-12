package org.hestiastore.index.segmentindex.core.bootstrap;

/**
 * One-use bootstrap step that initializes part of a segment index.
 * <p>
 * A step may keep resources created during {@link #apply} in its own fields.
 * When a later step fails, the bootstrap pipeline calls
 * {@link #closeResource()} in reverse order.
 * </p>
 *
 * @param <K> key type
 * @param <V> value type
 */
abstract class SegmentIndexBootstrapStep<K, V> {

    /**
     * Applies this bootstrap step.
     *
     * @param request immutable bootstrap inputs
     * @param state mutable bootstrap products
     */
    abstract void apply(SegmentIndexBootstrapRequest<K, V> request,
            SegmentIndexBootstrapState<K, V> state);

    /**
     * Closes resources created by this step after failed initialization.
     */
    void closeResource() {
        // Default step owns no closeable resource.
    }
}
