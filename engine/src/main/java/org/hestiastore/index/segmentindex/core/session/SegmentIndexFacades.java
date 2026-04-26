package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.routing.SegmentIndexDataAccess;
import org.hestiastore.index.segmentindex.core.routing.SegmentIndexMutationFacade;
import org.hestiastore.index.segmentindex.core.routing.SegmentIndexTrackedOperationRunner;
import org.hestiastore.index.segmentindex.core.streaming.SegmentIndexEntryIteratorDecorator;
import org.hestiastore.index.segmentindex.core.streaming.SegmentIndexReadFacade;

/**
 * Assembles data facades as one cohesive boundary so outer core composition
 * does not need to know facade-internal wiring details.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexFacades<K, V> {

    private final SegmentIndexMutationFacade<K, V> mutationFacade;
    private final SegmentIndexReadFacade<K, V> readFacade;

    private SegmentIndexFacades(
            final SegmentIndexMutationFacade<K, V> mutationFacade,
            final SegmentIndexReadFacade<K, V> readFacade) {
        this.mutationFacade = Vldtn.requireNonNull(mutationFacade,
                "mutationFacade");
        this.readFacade = Vldtn.requireNonNull(readFacade, "readFacade");
    }

    static <K, V> SegmentIndexFacades<K, V> create(
            final IndexConfiguration<K, V> conf,
            final SegmentIndexTrackedOperationRunner<K, V> trackedRunner,
            final SegmentIndexDataAccess<K, V> dataAccess) {
        final IndexConfiguration<K, V> validatedConfiguration = Vldtn
                .requireNonNull(conf, "conf");
        final SegmentIndexTrackedOperationRunner<K, V> validatedTrackedRunner =
                Vldtn.requireNonNull(trackedRunner, "trackedRunner");
        final SegmentIndexDataAccess<K, V> validatedDataAccess = Vldtn
                .requireNonNull(dataAccess, "dataAccess");
        return new SegmentIndexFacades<>(
                new SegmentIndexMutationFacade<>(
                        validatedTrackedRunner,
                        validatedDataAccess),
                new SegmentIndexReadFacade<>(
                        validatedTrackedRunner,
                        validatedDataAccess,
                        new SegmentIndexEntryIteratorDecorator<>(
                                validatedConfiguration)));
    }

    SegmentIndexMutationFacade<K, V> mutationFacade() {
        return mutationFacade;
    }

    SegmentIndexReadFacade<K, V> readFacade() {
        return readFacade;
    }
}
