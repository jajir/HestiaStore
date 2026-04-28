package org.hestiastore.index.segmentindex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.chunkstore.ChunkFilterRegistration;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.chunkstore.ChunkFilterSpecs;

/**
 * Builder section for persisted chunk filter pipelines.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexFilterConfigurationBuilder<K, V> {

    private final List<ChunkFilterSpec> encodingChunkFilters = new ArrayList<>();
    private final List<ChunkFilterSpec> decodingChunkFilters = new ArrayList<>();

    IndexFilterConfigurationBuilder() {
    }

    /**
     * Adds an encoding filter instance.
     *
     * @param value filter instance
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder<K, V> addEncodingFilter(
            final ChunkFilter value) {
        final ChunkFilter requiredFilter = Vldtn.requireNonNull(value,
                "filter");
        addEncodingFilter(() -> requiredFilter,
                resolvePersistableInstanceSpec(requiredFilter, true));
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
        final Class<? extends ChunkFilter> requiredClass = Vldtn
                .requireNonNull(value, "filterClass");
        addEncodingFilter(() -> instantiateFilter(requiredClass),
                ChunkFilterSpecs.forEncodingFilter(requiredClass));
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
        Vldtn.requireNonNull(supplier, "supplier");
        addEncodingFilter(spec);
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
        addEncodingFilterSpec(Vldtn.requireNonNull(value, "spec"));
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
        Vldtn.requireNonNull(values, "registrations");
        encodingChunkFilters.clear();
        for (final ChunkFilterRegistration registration : values) {
            addEncodingFilterSpec(Vldtn.requireNonNull(registration,
                    "registration").getSpec());
        }
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
        Vldtn.requireNonNull(values, "specs");
        encodingChunkFilters.clear();
        for (final ChunkFilterSpec spec : values) {
            addEncodingFilterSpec(spec);
        }
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
        Vldtn.requireNonNull(values, "filterClasses");
        encodingChunkFilters.clear();
        for (final Class<? extends ChunkFilter> filterClass : values) {
            addEncodingFilter(filterClass);
        }
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
        Vldtn.requireNonNull(values, "filters");
        encodingChunkFilters.clear();
        for (final ChunkFilter filter : values) {
            addEncodingFilter(filter);
        }
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
        final ChunkFilter requiredFilter = Vldtn.requireNonNull(value,
                "filter");
        addDecodingFilter(() -> requiredFilter,
                resolvePersistableInstanceSpec(requiredFilter, false));
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
        final Class<? extends ChunkFilter> requiredClass = Vldtn
                .requireNonNull(value, "filterClass");
        addDecodingFilter(() -> instantiateFilter(requiredClass),
                ChunkFilterSpecs.forDecodingFilter(requiredClass));
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
        Vldtn.requireNonNull(supplier, "supplier");
        addDecodingFilter(spec);
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
        addDecodingFilterSpec(Vldtn.requireNonNull(value, "spec"));
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
        Vldtn.requireNonNull(values, "registrations");
        decodingChunkFilters.clear();
        for (final ChunkFilterRegistration registration : values) {
            addDecodingFilterSpec(Vldtn.requireNonNull(registration,
                    "registration").getSpec());
        }
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
        Vldtn.requireNonNull(values, "specs");
        decodingChunkFilters.clear();
        for (final ChunkFilterSpec spec : values) {
            addDecodingFilterSpec(spec);
        }
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
        Vldtn.requireNonNull(values, "filterClasses");
        decodingChunkFilters.clear();
        for (final Class<? extends ChunkFilter> filterClass : values) {
            addDecodingFilter(filterClass);
        }
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
        Vldtn.requireNonNull(values, "filters");
        decodingChunkFilters.clear();
        for (final ChunkFilter filter : values) {
            addDecodingFilter(filter);
        }
        return this;
    }

    IndexFilterConfiguration build() {
        return new IndexFilterConfiguration(encodingChunkFilters,
                decodingChunkFilters);
    }

    private void addEncodingFilterSpec(final ChunkFilterSpec spec) {
        encodingChunkFilters.add(Vldtn.requireNonNull(spec, "spec"));
    }

    private void addDecodingFilterSpec(final ChunkFilterSpec spec) {
        decodingChunkFilters.add(Vldtn.requireNonNull(spec, "spec"));
    }

    private ChunkFilter instantiateFilter(
            final Class<? extends ChunkFilter> filterClass) {
        final Class<? extends ChunkFilter> requiredClass = Vldtn
                .requireNonNull(filterClass, "filterClass");
        try {
            return requiredClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException ex) {
            throw new IllegalArgumentException(
                    String.format("Unable to instantiate chunk filter '%s'",
                            requiredClass.getName()),
                    ex);
        }
    }

    private ChunkFilterSpec resolvePersistableInstanceSpec(
            final ChunkFilter filter, final boolean encoding) {
        final ChunkFilter requiredFilter = Vldtn.requireNonNull(filter,
                "filter");
        final ChunkFilterSpec spec = encoding
                ? ChunkFilterSpecs.forEncodingFilter(requiredFilter)
                : ChunkFilterSpecs.forDecodingFilter(requiredFilter);
        if (ChunkFilterProviderRegistry.PROVIDER_ID_JAVA_CLASS
                .equals(spec.getProviderId())) {
            throw new IllegalArgumentException(String.format(
                    "Custom %s chunk filter instances require explicit persisted metadata. "
                            + "Use %s(Supplier<? extends ChunkFilter>, ChunkFilterSpec) "
                            + "or %s(Class<? extends ChunkFilter>) for no-arg filters.",
                    encoding ? "encoding" : "decoding",
                    encoding ? "addEncodingFilter" : "addDecodingFilter",
                    encoding ? "addEncodingFilter" : "addDecodingFilter"));
        }
        return spec;
    }
}
