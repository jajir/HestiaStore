package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.configuration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.effectiveConfiguration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.saveConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.function.Supplier;

import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterProvider;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolverImpl;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationResolution;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStorage;
import org.junit.jupiter.api.Test;

class BootstrapConfigurationAccessTest {

    private static final String CUSTOM_PROVIDER_ID = "custom-bootstrap";

    private final BootstrapConfigurationAccess<Integer, String> access =
            new BootstrapConfigurationAccess<>();

    @Test
    void resolve_resolvesCreateConfigurationAndMarksWriteRequired() {
        final MemDirectory directory = new MemDirectory();

        final IndexConfigurationResolution<Integer, String> resolution =
                access.resolve(request(directory,
                        configuration("bootstrap-access-create"),
                        SegmentIndexBootstrapMode.CREATE));

        assertEquals("bootstrap-access-create",
                resolution.configuration().identity().name());
        assertTrue(resolution.writeRequired());
    }

    @Test
    void resolve_resolvesOpenConfigurationAndMarksCleanWhenUnchanged() {
        final MemDirectory directory = new MemDirectory();
        saveConfiguration(directory,
                configuration("bootstrap-access-open", false, 1));

        final IndexConfigurationResolution<Integer, String> resolution =
                access.resolve(request(directory,
                        configuration("bootstrap-access-open", false, 1),
                        SegmentIndexBootstrapMode.OPEN));

        assertEquals("bootstrap-access-open",
                resolution.configuration().identity().name());
        assertFalse(resolution.writeRequired());
    }

    @Test
    void resolve_usesRequestResolverWhenLoadingCustomFilterSpecs() {
        final MemDirectory directory = new MemDirectory();
        final ChunkFilterProviderResolver resolver = customFilterResolver();
        final ChunkFilterSpec filterSpec = ChunkFilterSpec.ofProvider(
                CUSTOM_PROVIDER_ID);
        final IndexConfiguration<Integer, String> configuration =
                customFilterConfiguration("bootstrap-access-custom",
                        filterSpec);
        new IndexConfigurationStorage<Integer, String>(directory, resolver)
                .save(EffectiveIndexConfigurationTestSupport.effective(
                        configuration, resolver));
        final SegmentIndexBootstrapRequest<Integer, String> request =
                new SegmentIndexBootstrapRequest<>(directory, configuration,
                        resolver, SegmentIndexBootstrapMode.OPEN);

        final IndexConfigurationResolution<Integer, String> resolution =
                access.resolve(request);

        assertEquals(List.of(filterSpec), resolution.configuration().filters()
                .encodingChunkFilterSpecs());
        assertEquals(List.of(filterSpec), resolution.configuration().filters()
                .decodingChunkFilterSpecs());
        assertFalse(resolution.writeRequired());
    }

    @Test
    void save_persistsConfiguration() {
        final MemDirectory directory = new MemDirectory();

        access.save(request(directory, SegmentIndexBootstrapMode.CREATE),
                effectiveConfiguration("bootstrap-access-save"));

        assertEquals("bootstrap-access-save",
                new IndexConfigurationStorage<Integer, String>(directory)
                        .load().identity().name());
    }

    private IndexConfiguration<Integer, String> customFilterConfiguration(
            final String indexName, final ChunkFilterSpec filterSpec) {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity
                        .keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity
                        .valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name(indexName))
                .filters(filters -> filters.addEncodingFilter(filterSpec))
                .filters(filters -> filters.addDecodingFilter(filterSpec))
                .build();
    }

    private ChunkFilterProviderResolver customFilterResolver() {
        return ChunkFilterProviderResolverImpl.builder().withDefaultProviders()
                .withProvider(new CustomChunkFilterProvider()).build();
    }

    private static final class CustomChunkFilterProvider
            implements ChunkFilterProvider {

        @Override
        public String getProviderId() {
            return CUSTOM_PROVIDER_ID;
        }

        @Override
        public Supplier<? extends ChunkFilter> createEncodingSupplier(
                final ChunkFilterSpec spec) {
            return ChunkFilterDoNothing::new;
        }

        @Override
        public Supplier<? extends ChunkFilter> createDecodingSupplier(
                final ChunkFilterSpec spec) {
            return ChunkFilterDoNothing::new;
        }
    }
}
