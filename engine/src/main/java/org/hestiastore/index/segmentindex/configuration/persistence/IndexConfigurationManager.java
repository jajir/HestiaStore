package org.hestiastore.index.segmentindex.configuration.persistence;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationResolver;

/**
 * Loads, resolves, merges, and persists effective index configuration values.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class IndexConfigurationManager<K, V> {

    private final IndexConfigurationStorage<K, V> confStorage;

    public IndexConfigurationManager(
            final IndexConfigurationStorage<K, V> confStorage) {
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

    public IndexConfigurationResolution<K, V> resolveForCreate(
            final IndexConfiguration<K, V> request) {
        return IndexConfigurationResolution.of(
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
        final IndexConfigurationResolution<K, V> resolution = resolveForOpen(
                request);
        if (resolution.writeRequired()) {
            confStorage.save(resolution.configuration());
        }
        return resolution.configuration();
    }

    public IndexConfigurationResolution<K, V> resolveForOpen(
            final IndexConfiguration<K, V> request) {
        final EffectiveIndexConfiguration<K, V> stored = confStorage.load();
        final EffectiveIndexConfiguration<K, V> merged =
                EffectiveIndexConfigurationResolver.mergeWithStored(stored,
                        request, confStorage.chunkFilterProviderResolver());
        return IndexConfigurationResolution.of(merged,
                !sameConfiguration(stored, merged));
    }

    private boolean sameConfiguration(
            final EffectiveIndexConfiguration<K, V> left,
            final EffectiveIndexConfiguration<K, V> right) {
        return sameIdentity(left, right)
                && sameSegment(left, right)
                && sameWritePath(left, right)
                && sameBloomFilter(left, right)
                && sameMaintenance(left, right)
                && sameIo(left, right)
                && sameLogging(left, right)
                && sameChunkStoreCache(left, right)
                && left.wal().equals(right.wal())
                && sameFilters(left.filters().encodingChunkFilterSpecs(),
                        right.filters().encodingChunkFilterSpecs())
                && sameFilters(left.filters().decodingChunkFilterSpecs(),
                        right.filters().decodingChunkFilterSpecs());
    }

    private boolean sameIdentity(
            final EffectiveIndexConfiguration<K, V> left,
            final EffectiveIndexConfiguration<K, V> right) {
        return left.identity().name().equals(right.identity().name())
                && left.identity().keyClass().equals(right.identity().keyClass())
                && left.identity().valueClass()
                        .equals(right.identity().valueClass())
                && left.identity().keyTypeDescriptor()
                        .equals(right.identity().keyTypeDescriptor())
                && left.identity().valueTypeDescriptor()
                        .equals(right.identity().valueTypeDescriptor());
    }

    private boolean sameSegment(
            final EffectiveIndexConfiguration<K, V> left,
            final EffectiveIndexConfiguration<K, V> right) {
        return left.segment().maxKeys() == right.segment().maxKeys()
                && left.segment().chunkKeyLimit() == right.segment()
                        .chunkKeyLimit()
                && left.segment().cacheKeyLimit() == right.segment()
                        .cacheKeyLimit()
                && left.segment().cachedSegmentLimit() == right.segment()
                        .cachedSegmentLimit()
                && left.segment().deltaCacheFileLimit() == right.segment()
                        .deltaCacheFileLimit();
    }

    private boolean sameWritePath(
            final EffectiveIndexConfiguration<K, V> left,
            final EffectiveIndexConfiguration<K, V> right) {
        return left.writePath().segmentWriteCacheKeyLimit() == right.writePath()
                .segmentWriteCacheKeyLimit()
                && left.writePath()
                        .segmentWriteCacheKeyLimitDuringMaintenance() == right
                                .writePath()
                                .segmentWriteCacheKeyLimitDuringMaintenance()
                && left.writePath().indexBufferedWriteKeyLimit() == right
                        .writePath().indexBufferedWriteKeyLimit()
                && left.writePath().segmentSplitKeyThreshold() == right
                        .writePath().segmentSplitKeyThreshold();
    }

    private boolean sameBloomFilter(
            final EffectiveIndexConfiguration<K, V> left,
            final EffectiveIndexConfiguration<K, V> right) {
        return left.bloomFilter().hashFunctions() == right.bloomFilter()
                .hashFunctions()
                && left.bloomFilter().indexSizeBytes() == right.bloomFilter()
                        .indexSizeBytes()
                && Double.compare(left.bloomFilter()
                        .falsePositiveProbability(),
                        right.bloomFilter()
                                .falsePositiveProbability()) == 0;
    }

    private boolean sameMaintenance(
            final EffectiveIndexConfiguration<K, V> left,
            final EffectiveIndexConfiguration<K, V> right) {
        return left.maintenance().segmentThreads() == right.maintenance()
                .segmentThreads()
                && left.maintenance().indexThreads() == right.maintenance()
                        .indexThreads()
                && left.maintenance().registryLifecycleThreads() == right
                        .maintenance().registryLifecycleThreads()
                && left.maintenance().busyBackoffMillis() == right
                        .maintenance().busyBackoffMillis()
                && left.maintenance().busyTimeoutMillis() == right
                        .maintenance().busyTimeoutMillis()
                && left.maintenance().backgroundAutoEnabled() == right
                        .maintenance().backgroundAutoEnabled();
    }

    private boolean sameIo(final EffectiveIndexConfiguration<K, V> left,
            final EffectiveIndexConfiguration<K, V> right) {
        return left.io().diskBufferSizeBytes() == right.io()
                .diskBufferSizeBytes();
    }

    private boolean sameLogging(
            final EffectiveIndexConfiguration<K, V> left,
            final EffectiveIndexConfiguration<K, V> right) {
        return left.logging().contextEnabled() == right.logging()
                .contextEnabled();
    }

    private boolean sameChunkStoreCache(
            final EffectiveIndexConfiguration<K, V> left,
            final EffectiveIndexConfiguration<K, V> right) {
        return left.chunkStoreCache().pageLimit() == right.chunkStoreCache()
                .pageLimit();
    }

    private boolean sameFilters(final List<ChunkFilterSpec> left,
            final List<ChunkFilterSpec> right) {
        return left.equals(right);
    }
}
