package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.session.IndexInternal;

/**
 * Opens one segment-index bootstrap operation and owns cleanup until the
 * running index is returned to the caller.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexBootstrapOperation<K, V> {

    private final Directory directory;
    private final IndexConfiguration<K, V> userProvidedConfiguration;
    private final ChunkFilterProviderResolver chunkFilterProviderResolver;

    static <K, V> SegmentIndexBootstrapOperation<K, V> create(
            final Directory directory,
            final IndexConfiguration<K, V> userProvidedConfiguration,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return new SegmentIndexBootstrapOperation<>(directory,
                userProvidedConfiguration, chunkFilterProviderResolver);
    }

    private SegmentIndexBootstrapOperation(final Directory directory,
            final IndexConfiguration<K, V> userProvidedConfiguration,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.userProvidedConfiguration = Vldtn.requireNonNull(
                userProvidedConfiguration, "userProvidedConfiguration");
        this.chunkFilterProviderResolver = chunkFilterProviderResolver;
    }

    IndexInternal<K, V> create() {
        return openSession(SegmentIndexBootstrapMode.CREATE).requireIndex();
    }

    IndexInternal<K, V> open() {
        return openSession(SegmentIndexBootstrapMode.OPEN).requireIndex();
    }

    Optional<IndexInternal<K, V>> tryOpen() {
        return openSession(SegmentIndexBootstrapMode.TRY_OPEN).index();
    }

    private SegmentIndexBootstrapResult<K, V> openSession(
            final SegmentIndexBootstrapMode mode) {
        final SegmentIndexBootstrapRequest<K, V> request = new SegmentIndexBootstrapRequest<>(directory,
                userProvidedConfiguration, chunkFilterProviderResolver,
                mode);
        final SegmentIndexBootstrapState<K, V> state = new SegmentIndexBootstrapState<>();
        return new SegmentIndexBootstrapPipeline<>(
                SegmentIndexBootstrapSteps.<K, V>startingSteps())
                .run(request, state);
    }
}
