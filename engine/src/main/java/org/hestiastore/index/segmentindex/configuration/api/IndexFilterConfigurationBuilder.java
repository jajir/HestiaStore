package org.hestiastore.index.segmentindex.configuration.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.chunkstore.ChunkFilterRegistration;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.chunkstore.ChunkFilterSpecs;

/**
 * Builder section for persisted chunk filter pipelines.
 */
public final class IndexFilterConfigurationBuilder {

    private static final String FILTER_ARG = "filter";
    private static final String FILTER_CLASS_ARG = "filterClass";

    private final List<ChunkFilterSpec> encodingChunkFilters = new ArrayList<>();
    private final List<ChunkFilterSpec> decodingChunkFilters = new ArrayList<>();
    private ChunkFilterProviderResolver chunkFilterProviderResolver;

    IndexFilterConfigurationBuilder() {
    }

    /**
     * Adds an encoding filter instance.
     *
     * @param value filter instance
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder addEncodingFilter(
            final ChunkFilter value) {
        final ChunkFilter requiredFilter = Vldtn.requireNonNull(value,
                FILTER_ARG);
        addEncodingFilter(resolvePersistableInstanceSpec(requiredFilter, true));
        return this;
    }

    /**
     * Adds an encoding filter class.
     *
     * @param value filter class
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder addEncodingFilter(
            final Class<? extends ChunkFilter> value) {
        final Class<? extends ChunkFilter> requiredClass = Vldtn
                .requireNonNull(value, FILTER_CLASS_ARG);
        addEncodingFilter(ChunkFilterSpecs.forEncodingFilter(requiredClass));
        return this;
    }

    /**
     * Adds an encoding filter spec.
     *
     * @param value filter spec
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder addEncodingFilter(
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
    public IndexFilterConfigurationBuilder encodingFilterRegistrations(
            final Collection<ChunkFilterRegistration> values) {
        replaceSpecs(values, "registrations", "registration",
                encodingChunkFilters, ChunkFilterRegistration::getSpec);
        return this;
    }

    /**
     * Replaces encoding filter specs.
     *
     * @param values filter specs
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder encodingFilterSpecs(
            final Collection<ChunkFilterSpec> values) {
        replaceSpecs(values, "specs", "spec", encodingChunkFilters,
                Function.identity());
        return this;
    }

    /**
     * Replaces encoding filter classes.
     *
     * @param values filter classes
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder encodingFilterClasses(
            final Collection<Class<? extends ChunkFilter>> values) {
        replaceSpecs(values, "filterClasses", FILTER_CLASS_ARG,
                encodingChunkFilters, ChunkFilterSpecs::forEncodingFilter);
        return this;
    }

    /**
     * Replaces encoding filters.
     *
     * @param values filters
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder encodingFilters(
            final Collection<ChunkFilter> values) {
        replaceSpecs(values, "filters", FILTER_ARG, encodingChunkFilters,
                filter -> resolvePersistableInstanceSpec(filter, true));
        return this;
    }

    /**
     * Adds a decoding filter instance.
     *
     * @param value filter instance
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder addDecodingFilter(
            final ChunkFilter value) {
        final ChunkFilter requiredFilter = Vldtn.requireNonNull(value,
                FILTER_ARG);
        addDecodingFilter(resolvePersistableInstanceSpec(requiredFilter, false));
        return this;
    }

    /**
     * Adds a decoding filter class.
     *
     * @param value filter class
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder addDecodingFilter(
            final Class<? extends ChunkFilter> value) {
        final Class<? extends ChunkFilter> requiredClass = Vldtn
                .requireNonNull(value, FILTER_CLASS_ARG);
        addDecodingFilter(ChunkFilterSpecs.forDecodingFilter(requiredClass));
        return this;
    }

    /**
     * Adds a decoding filter spec.
     *
     * @param value filter spec
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder addDecodingFilter(
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
    public IndexFilterConfigurationBuilder decodingFilterRegistrations(
            final Collection<ChunkFilterRegistration> values) {
        replaceSpecs(values, "registrations", "registration",
                decodingChunkFilters, ChunkFilterRegistration::getSpec);
        return this;
    }

    /**
     * Replaces decoding filter specs.
     *
     * @param values filter specs
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder decodingFilterSpecs(
            final Collection<ChunkFilterSpec> values) {
        replaceSpecs(values, "specs", "spec", decodingChunkFilters,
                Function.identity());
        return this;
    }

    /**
     * Replaces decoding filter classes.
     *
     * @param values filter classes
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder decodingFilterClasses(
            final Collection<Class<? extends ChunkFilter>> values) {
        replaceSpecs(values, "filterClasses", FILTER_CLASS_ARG,
                decodingChunkFilters, ChunkFilterSpecs::forDecodingFilter);
        return this;
    }

    /**
     * Replaces decoding filters.
     *
     * @param values filters
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder decodingFilters(
            final Collection<ChunkFilter> values) {
        replaceSpecs(values, "filters", FILTER_ARG, decodingChunkFilters,
                filter -> resolvePersistableInstanceSpec(filter, false));
        return this;
    }

    /**
     * Configures the runtime resolver used to materialize persisted chunk
     * filter specs.
     *
     * @param value runtime chunk filter provider resolver
     * @return this section builder
     */
    public IndexFilterConfigurationBuilder chunkFilterProviderResolver(
            final ChunkFilterProviderResolver value) {
        this.chunkFilterProviderResolver = Vldtn.requireNonNull(value,
                "chunkFilterProviderResolver");
        return this;
    }

    IndexFilterConfiguration build() {
        return new IndexFilterConfiguration(encodingChunkFilters,
                decodingChunkFilters, chunkFilterProviderResolver);
    }

    private void addEncodingFilterSpec(final ChunkFilterSpec spec) {
        encodingChunkFilters.add(Vldtn.requireNonNull(spec, "spec"));
    }

    private void addDecodingFilterSpec(final ChunkFilterSpec spec) {
        decodingChunkFilters.add(Vldtn.requireNonNull(spec, "spec"));
    }

    private static <T> void replaceSpecs(final Collection<T> values,
            final String valuesName, final String valueName,
            final List<ChunkFilterSpec> target,
            final Function<T, ChunkFilterSpec> mapper) {
        Vldtn.requireNonNull(values, valuesName);
        target.clear();
        for (final T value : values) {
            final T nonNullValue = Vldtn.requireNonNull(value, valueName);
            target.add(Vldtn.requireNonNull(mapper.apply(nonNullValue),
                    "spec"));
        }
    }

    private ChunkFilterSpec resolvePersistableInstanceSpec(
            final ChunkFilter filter, final boolean encoding) {
        final ChunkFilter requiredFilter = Vldtn.requireNonNull(filter,
                FILTER_ARG);
        final ChunkFilterSpec spec = encoding
                ? ChunkFilterSpecs.forEncodingFilter(requiredFilter)
                : ChunkFilterSpecs.forDecodingFilter(requiredFilter);
        if (ChunkFilterProviderResolver.PROVIDER_ID_JAVA_CLASS
                .equals(spec.getProviderId())) {
            throw new IllegalArgumentException(String.format(
                    "Custom %s chunk filter instances require explicit persisted metadata. "
                            + "Use %s(ChunkFilterSpec) "
                            + "or %s(Class<? extends ChunkFilter>) for no-arg filters.",
                    encoding ? "encoding" : "decoding",
                    encoding ? "addEncodingFilter" : "addDecodingFilter",
                    encoding ? "addEncodingFilter" : "addDecodingFilter"));
        }
        return spec;
    }
}
