package org.hestiastore.indextools;

import java.util.ServiceLoader;

import org.hestiastore.index.chunkstore.ChunkFilterProvider;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;

final class ClasspathExtensionSupport {

    private ClasspathExtensionSupport() {
    }

    static ChunkFilterProviderRegistry chunkFilterProviderRegistry() {
        final ChunkFilterProviderRegistry.Builder builder = ChunkFilterProviderRegistry
                .builder().withDefaultProviders();
        final ServiceLoader<ChunkFilterProvider> loader = ServiceLoader
                .load(ChunkFilterProvider.class);
        for (final ChunkFilterProvider provider : loader) {
            builder.withProvider(provider);
        }
        return builder.build();
    }

    static String toolVersion() {
        final Package toolPackage = IndexTool.class.getPackage();
        final String implementationVersion = toolPackage == null ? null
                : toolPackage.getImplementationVersion();
        return implementationVersion == null ? "dev" : implementationVersion;
    }
}
