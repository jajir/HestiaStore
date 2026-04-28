package org.hestiastore.index.segmentindex;

import java.util.Collection;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterRegistration;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;

/**
 * Builder section for persisted chunk filter pipelines.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexFilterConfigurationBuilder<K, V> {

    private final IndexConfigurationBuilder<K, V> builder;

    IndexFilterConfigurationBuilder(
            final IndexConfigurationBuilder<K, V> builder) {
        this.builder = Vldtn.requireNonNull(builder, "builder");
    }

    /**
     * Adds an encoding filter instance.
     *
     * @param value filter instance
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder<K, V> addEncodingFilter(
            final ChunkFilter value) {
        builder.addEncodingFilter(value);
        return this;
    }

    /**
     * Adds an encoding filter class.
     *
     * @param value filter class
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder<K, V> addEncodingFilter(
            final Class<? extends ChunkFilter> value) {
        builder.addEncodingFilter(value);
        return this;
    }

    /**
     * Adds an encoding filter supplier and persisted spec.
     *
     * @param supplier runtime supplier
     * @param spec persisted spec
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder<K, V> addEncodingFilter(
            final Supplier<? extends ChunkFilter> supplier,
            final ChunkFilterSpec spec) {
        builder.addEncodingFilter(supplier, spec);
        return this;
    }

    /**
     * Adds an encoding filter spec.
     *
     * @param value filter spec
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder<K, V> addEncodingFilter(
            final ChunkFilterSpec value) {
        builder.addEncodingFilter(value);
        return this;
    }

    /**
     * Replaces encoding filter registrations.
     *
     * @param values filter registrations
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder<K, V> encodingFilterRegistrations(
            final Collection<ChunkFilterRegistration> values) {
        builder.setEncodingFilterRegistrations(values);
        return this;
    }

    /**
     * Replaces encoding filter specs.
     *
     * @param values filter specs
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder<K, V> encodingFilterSpecs(
            final Collection<ChunkFilterSpec> values) {
        builder.setEncodingFilterSpecs(values);
        return this;
    }

    /**
     * Replaces encoding filter classes.
     *
     * @param values filter classes
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder<K, V> encodingFilterClasses(
            final Collection<Class<? extends ChunkFilter>> values) {
        builder.setEncodingFilterClasses(values);
        return this;
    }

    /**
     * Replaces encoding filters.
     *
     * @param values filters
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder<K, V> encodingFilters(
            final Collection<ChunkFilter> values) {
        builder.setEncodingFilters(values);
        return this;
    }

    /**
     * Adds a decoding filter instance.
     *
     * @param value filter instance
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder<K, V> addDecodingFilter(
            final ChunkFilter value) {
        builder.addDecodingFilter(value);
        return this;
    }

    /**
     * Adds a decoding filter class.
     *
     * @param value filter class
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder<K, V> addDecodingFilter(
            final Class<? extends ChunkFilter> value) {
        builder.addDecodingFilter(value);
        return this;
    }

    /**
     * Adds a decoding filter supplier and persisted spec.
     *
     * @param supplier runtime supplier
     * @param spec persisted spec
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder<K, V> addDecodingFilter(
            final Supplier<? extends ChunkFilter> supplier,
            final ChunkFilterSpec spec) {
        builder.addDecodingFilter(supplier, spec);
        return this;
    }

    /**
     * Adds a decoding filter spec.
     *
     * @param value filter spec
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder<K, V> addDecodingFilter(
            final ChunkFilterSpec value) {
        builder.addDecodingFilter(value);
        return this;
    }

    /**
     * Replaces decoding filter registrations.
     *
     * @param values filter registrations
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder<K, V> decodingFilterRegistrations(
            final Collection<ChunkFilterRegistration> values) {
        builder.setDecodingFilterRegistrations(values);
        return this;
    }

    /**
     * Replaces decoding filter specs.
     *
     * @param values filter specs
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder<K, V> decodingFilterSpecs(
            final Collection<ChunkFilterSpec> values) {
        builder.setDecodingFilterSpecs(values);
        return this;
    }

    /**
     * Replaces decoding filter classes.
     *
     * @param values filter classes
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder<K, V> decodingFilterClasses(
            final Collection<Class<? extends ChunkFilter>> values) {
        builder.setDecodingFilterClasses(values);
        return this;
    }

    /**
     * Replaces decoding filters.
     *
     * @param values filters
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder<K, V> decodingFilters(
            final Collection<ChunkFilter> values) {
        builder.setDecodingFilters(values);
        return this;
    }
}
