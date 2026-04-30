package org.hestiastore.index.segmentindex;

import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;

/**
 * Immutable configuration for the segment-index layer.
 * <p>
 * Encapsulates key/value types, index name, segment sizing and caching limits,
 * Bloom filter parameters, disk I/O buffer size, logging switches, and the
 * chunk encoding/decoding filter pipeline metadata. Instances are created via
 * the fluent {@link IndexConfigurationBuilder}.
 *
 * @param <K> key type
 * @param <V> value type
 * @see IndexConfigurationBuilder
 */
@SuppressWarnings("java:S107")
public class IndexConfiguration<K, V> {

    private final IndexIdentityConfiguration<K, V> identity;
    private final IndexSegmentConfiguration segment;
    private final IndexRuntimeTuningConfiguration runtimeTuning;
    private final IndexBloomFilterConfiguration bloomFilter;
    private final IndexMaintenanceConfiguration maintenance;
    private final IndexIoConfiguration io;
    private final IndexLoggingConfiguration logging;
    private final IndexWalConfiguration wal;
    private final IndexFilterConfiguration filters;

    /**
     * Creates a new instance of IndexConfigurationBuilder.
     *
     * @param <M> the type of the key
     * @param <N> the type of the value
     * @return a new instance of IndexConfigurationBuilder
     */
    public static <M, N> IndexConfigurationBuilder<M, N> builder() {
        return new IndexConfigurationBuilder<>();
    }

    IndexConfiguration(final IndexIdentityConfiguration<K, V> identity,
            final IndexSegmentConfiguration segment,
            final IndexRuntimeTuningConfiguration runtimeTuning,
            final IndexBloomFilterConfiguration bloomFilter,
            final IndexMaintenanceConfiguration maintenance,
            final IndexIoConfiguration io,
            final IndexLoggingConfiguration logging,
            final IndexWalConfiguration wal,
            final IndexFilterConfiguration filters) {
        this.identity = identity;
        this.segment = segment;
        this.runtimeTuning = runtimeTuning;
        this.bloomFilter = bloomFilter;
        this.maintenance = maintenance;
        this.io = io;
        this.logging = logging;
        this.wal = IndexWalConfiguration.orEmpty(wal);
        this.filters = filters;
    }

    /**
     * Returns grouped identity and key/value type metadata.
     *
     * @return immutable identity view
     */
    public IndexIdentityConfiguration<K, V> identity() {
        return identity;
    }

    /**
     * Returns grouped segment sizing and cache settings.
     *
     * @return immutable segment settings view
     */
    public IndexSegmentConfiguration segment() {
        return segment;
    }

    /**
     * Returns canonical direct-to-segment write-path settings.
     *
     * @return immutable write-path settings
     */
    public IndexWritePathConfiguration writePath() {
        return runtimeTuning.writePath();
    }

    /**
     * Returns grouped Bloom filter settings.
     *
     * @return immutable Bloom filter settings view
     */
    public IndexBloomFilterConfiguration bloomFilter() {
        return bloomFilter;
    }

    /**
     * Returns grouped maintenance and retry settings.
     *
     * @return immutable maintenance settings view
     */
    public IndexMaintenanceConfiguration maintenance() {
        return maintenance;
    }

    /**
     * Returns grouped disk I/O settings.
     *
     * @return immutable I/O settings view
     */
    public IndexIoConfiguration io() {
        return io;
    }

    /**
     * Returns grouped logging settings.
     *
     * @return immutable logging settings view
     */
    public IndexLoggingConfiguration logging() {
        return logging;
    }

    /**
     * Returns WAL settings.
     *
     * @return non-null WAL settings
     */
    public IndexWalConfiguration wal() {
        return wal;
    }

    /**
     * Returns grouped persisted chunk filter settings.
     *
     * @return immutable filter settings view
     */
    public IndexFilterConfiguration filters() {
        return filters;
    }

    /**
     * Returns grouped runtime-tunable settings derived from this
     * configuration.
     *
     * @return immutable runtime tuning settings view
     */
    public IndexRuntimeTuningConfiguration runtimeTuning() {
        return runtimeTuning;
    }

    /**
     * Resolves this persisted configuration into runtime filter suppliers using
     * the resolver configured in the filter section, or the default resolver
     * when none was specified.
     *
     * @return runtime configuration resolved from persisted metadata
     */
    public IndexRuntimeConfiguration<K, V> resolveRuntimeConfiguration() {
        return resolveRuntimeConfiguration(
                filters.getChunkFilterProviderResolver());
    }

    /**
     * Resolves this persisted configuration into runtime filter suppliers using
     * the provided chunk filter provider resolver.
     *
     * @param chunkFilterProviderResolver resolver used to resolve persisted
     *                                    chunk filter specs
     * @return runtime configuration resolved from persisted metadata
     */
    public IndexRuntimeConfiguration<K, V> resolveRuntimeConfiguration(
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return IndexRuntimeConfiguration.resolve(this,
                chunkFilterProviderResolver);
    }

}
