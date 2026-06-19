package org.hestiastore.index.segmentindex.configuration.persistence;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationResolver;

/**
 * Loads, resolves, merges, and persists effective index configuration values.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class IndexConfigurationManager<K, V> {

    private final IndexConfigurationStore<K, V> confStorage;

    public IndexConfigurationManager(
            final IndexConfigurationStore<K, V> confStorage) {
        this.confStorage = Vldtn.requireNonNull(confStorage, "confStorage");
    }

    public EffectiveIndexConfiguration<K, V> loadExisting() {
        return confStorage.load();
    }

    public Optional<EffectiveIndexConfiguration<K, V>> tryToLoad() {
        if (confStorage.exists()) {
            return Optional.of(confStorage.load());
        }
        return Optional.empty();
    }

    public EffectiveIndexConfiguration<K, V> applyDefaults(
            final IndexConfiguration<K, V> request) {
        return resolveForCreate(request).configuration();
    }

    public ResolvedIndexConfiguration<K, V> resolveForCreate(
            final IndexConfiguration<K, V> request) {
        return ResolvedIndexConfiguration.of(
                EffectiveIndexConfigurationResolver.resolveForCreate(request,
                        confStorage.chunkFilterProviderResolver()),
                true);
    }

    public void save(
            final EffectiveIndexConfiguration<K, V> effectiveConfiguration) {
        confStorage.save(Vldtn.requireNonNull(effectiveConfiguration,
                "effectiveConfiguration"));
    }

    public EffectiveIndexConfiguration<K, V> mergeWithStored(
            final IndexConfiguration<K, V> request) {
        final ResolvedIndexConfiguration<K, V> resolution = resolveForOpen(
                request);
        if (resolution.writeRequired()) {
            confStorage.save(resolution.configuration());
        }
        return resolution.configuration();
    }

    public ResolvedIndexConfiguration<K, V> resolveForOpen(
            final IndexConfiguration<K, V> request) {
        final EffectiveIndexConfiguration<K, V> stored = confStorage.load();
        final EffectiveIndexConfiguration<K, V> merged =
                EffectiveIndexConfigurationResolver.mergeWithStored(stored,
                        request, confStorage.chunkFilterProviderResolver());
        return ResolvedIndexConfiguration.of(merged,
                !sameConfiguration(stored, merged));
    }

    private boolean sameConfiguration(
            final EffectiveIndexConfiguration<K, V> left,
            final EffectiveIndexConfiguration<K, V> right) {
        return canonicalConfiguration(left).equals(canonicalConfiguration(right));
    }

    private List<?> canonicalConfiguration(
            final EffectiveIndexConfiguration<K, V> configuration) {
        return List.of(
                configuration.identity().name(),
                configuration.identity().keyClass(),
                configuration.identity().valueClass(),
                configuration.identity().keyTypeDescriptor(),
                configuration.identity().valueTypeDescriptor(),
                configuration.segment().maxKeys(),
                configuration.segment().chunkKeyLimit(),
                configuration.segment().cacheKeyLimit(),
                configuration.segment().cachedSegmentLimit(),
                configuration.segment().deltaCacheFileLimit(),
                configuration.writePath().segmentWriteCacheKeyLimit(),
                configuration.writePath()
                        .segmentWriteCacheKeyLimitDuringMaintenance(),
                configuration.writePath().indexBufferedWriteKeyLimit(),
                configuration.writePath().segmentSplitKeyThreshold(),
                configuration.bloomFilter().hashFunctions(),
                configuration.bloomFilter().indexSizeBytes(),
                configuration.bloomFilter().falsePositiveProbability(),
                configuration.maintenance().segmentThreads(),
                configuration.maintenance().indexThreads(),
                configuration.maintenance().registryLifecycleThreads(),
                configuration.maintenance().busyBackoffMillis(),
                configuration.maintenance().busyTimeoutMillis(),
                configuration.maintenance().backgroundAutoEnabled(),
                configuration.io().diskBufferSizeBytes(),
                configuration.logging().contextEnabled(),
                configuration.chunkStoreCache().pageLimit(),
                configuration.wal(),
                configuration.filters().encodingChunkFilterSpecs(),
                configuration.filters().decodingChunkFilterSpecs());
    }
}
