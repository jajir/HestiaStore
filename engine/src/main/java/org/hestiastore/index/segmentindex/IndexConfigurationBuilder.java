package org.hestiastore.index.segmentindex;

import java.util.function.Consumer;

import org.hestiastore.index.Vldtn;

/**
 * Fluent builder for {@link IndexConfiguration} instances.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class IndexConfigurationBuilder<K, V> {

    private final IndexIdentityConfigurationBuilder<K, V> identity =
            new IndexIdentityConfigurationBuilder<>();
    private final IndexSegmentConfigurationBuilder<K, V> segment =
            new IndexSegmentConfigurationBuilder<>();
    private final IndexWritePathConfigurationBuilder<K, V> writePath =
            new IndexWritePathConfigurationBuilder<>();
    private final IndexBloomFilterConfigurationBuilder<K, V> bloomFilter =
            new IndexBloomFilterConfigurationBuilder<>();
    private final IndexWalConfigurationBuilder wal =
            new IndexWalConfigurationBuilder();
    private final IndexMaintenanceConfigurationBuilder<K, V> maintenance =
            new IndexMaintenanceConfigurationBuilder<>();
    private final IndexIoConfigurationBuilder<K, V> io =
            new IndexIoConfigurationBuilder<>();
    private final IndexLoggingConfigurationBuilder<K, V> logging =
            new IndexLoggingConfigurationBuilder<>();
    private final IndexFilterConfigurationBuilder<K, V> filters =
            new IndexFilterConfigurationBuilder<>();
    private boolean walConfigured;

    IndexConfigurationBuilder() {

    }

    /**
     * Configures index identity and type metadata.
     *
     * @param customizer identity section customizer
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> identity(
            final Consumer<IndexIdentityConfigurationBuilder<K, V>> customizer) {
        applyCustomizer(customizer, identity);
        return this;
    }

    /**
     * Configures segment sizing and cache settings.
     *
     * @param customizer segment section customizer
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> segment(
            final Consumer<IndexSegmentConfigurationBuilder<K, V>> customizer) {
        applyCustomizer(customizer, segment);
        return this;
    }

    /**
     * Configures direct-to-segment write path settings.
     *
     * @param customizer write-path section customizer
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> writePath(
            final Consumer<IndexWritePathConfigurationBuilder<K, V>> customizer) {
        applyCustomizer(customizer, writePath);
        return this;
    }

    /**
     * Configures Bloom filter settings.
     *
     * @param customizer Bloom filter section customizer
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> bloomFilter(
            final Consumer<IndexBloomFilterConfigurationBuilder<K, V>> customizer) {
        applyCustomizer(customizer, bloomFilter);
        return this;
    }

    /**
     * Configures WAL settings.
     *
     * @param customizer WAL section customizer
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> wal(
            final Consumer<IndexWalConfigurationBuilder> customizer) {
        applyCustomizer(customizer, wal);
        this.walConfigured = true;
        return this;
    }

    /**
     * Configures maintenance and retry settings.
     *
     * @param customizer maintenance section customizer
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> maintenance(
            final Consumer<IndexMaintenanceConfigurationBuilder<K, V>> customizer) {
        applyCustomizer(customizer, maintenance);
        return this;
    }

    /**
     * Configures I/O settings.
     *
     * @param customizer I/O section customizer
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> io(
            final Consumer<IndexIoConfigurationBuilder<K, V>> customizer) {
        applyCustomizer(customizer, io);
        return this;
    }

    /**
     * Configures logging settings.
     *
     * @param customizer logging section customizer
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> logging(
            final Consumer<IndexLoggingConfigurationBuilder<K, V>> customizer) {
        applyCustomizer(customizer, logging);
        return this;
    }

    /**
     * Configures persisted chunk filter pipelines.
     *
     * @param customizer filter section customizer
     * @return this builder
     */
    public IndexConfigurationBuilder<K, V> filters(
            final Consumer<IndexFilterConfigurationBuilder<K, V>> customizer) {
        applyCustomizer(customizer, filters);
        return this;
    }

    /**
     * Builds an immutable {@link IndexConfiguration} from the collected
     * settings.
     *
     * @return built configuration
     */
    public IndexConfiguration<K, V> build() {
        final IndexIdentityConfiguration<K, V> identityConfiguration =
                identity.build();
        final Integer effectiveSegmentMaxKeys =
                segment.resolveEffectiveMaxKeys(
                        writePath.segmentSplitKeyThreshold());
        final IndexSegmentConfiguration segmentConfiguration =
                segment.build(effectiveSegmentMaxKeys);
        final IndexWritePathConfiguration writePathConfiguration =
                writePath.build(segmentConfiguration.maxKeys(),
                        segmentConfiguration.cachedSegmentLimit());
        final IndexRuntimeTuningConfiguration runtimeTuningConfiguration =
                new IndexRuntimeTuningConfiguration(
                        segmentConfiguration.cachedSegmentLimit(),
                        segmentConfiguration.cacheKeyLimit(),
                        writePathConfiguration,
                        writePath.legacyImmutableRunLimit());
        return new IndexConfiguration<>(identityConfiguration,
                segmentConfiguration, runtimeTuningConfiguration,
                bloomFilter.build(), maintenance.build(), io.build(),
                logging.build(), buildWal(), filters.build());
    }

    private IndexWalConfiguration buildWal() {
        if (!walConfigured) {
            return IndexWalConfiguration.EMPTY;
        }
        return wal.build();
    }

    private static <T> void applyCustomizer(final Consumer<T> customizer,
            final T sectionBuilder) {
        Vldtn.requireNonNull(customizer, "customizer").accept(sectionBuilder);
    }
}
