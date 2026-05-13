package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.chunkstorecache.LruChunkStoreCache;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Opens the core storage collaborators needed before split and runtime services
 * can proceed.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexCoreStorageFactory<K, V> {

    private final SegmentIndexCoreStorageOpenSpec<K, V> openSpec;
    private final SegmentIndexCoreStorageOpenObserver<K, V> openObserver;

    public SegmentIndexCoreStorageFactory(
            final SegmentIndexCoreStorageOpenSpec<K, V> openSpec,
            final SegmentIndexCoreStorageOpenObserver<K, V> openObserver) {
        this.openSpec = Vldtn.requireNonNull(openSpec, "openSpec");
        this.openObserver = Vldtn.requireNonNull(openObserver,
                "openObserver");
    }

    public SegmentIndexCoreStorage<K, V> create() {
        final RuntimeTuningState runtimeTuningState = RuntimeTuningState
                .fromConfiguration(openSpec.conf());
        final KeyToSegmentMapImpl<K> keyToSegmentMapDelegate = new KeyToSegmentMapImpl<>(
                openSpec.directoryFacade(), openSpec.keyTypeDescriptor());
        final KeyToSegmentMap<K> keyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMapDelegate);
        openObserver.onKeyToSegmentMapCreated(keyToSegmentMap);
        final ChunkStoreCache<K, V> chunkStoreCache =
                newChunkStoreCache(openSpec.conf());
        final SegmentRegistry<K, V> segmentRegistry = newSegmentRegistry(
                chunkStoreCache);
        openObserver.onSegmentRegistryCreated(segmentRegistry);
        return new SegmentIndexCoreStorage<>(runtimeTuningState, keyToSegmentMap,
                segmentRegistry, chunkStoreCache,
                newRetryPolicy(openSpec.conf()));
    }

    private SegmentRegistry<K, V> newSegmentRegistry(
            final ChunkStoreCache<K, V> chunkStoreCache) {
        return SegmentRegistry.<K, V>builder()
                .withDirectoryFacade(openSpec.directoryFacade())
                .withKeyTypeDescriptor(openSpec.keyTypeDescriptor())
                .withValueTypeDescriptor(openSpec.valueTypeDescriptor())
                .withConfiguration(openSpec.conf())
                .withSegmentMaintenanceExecutor(
                        openSpec.executorRegistry()
                                .getStableSegmentMaintenanceExecutor())
                .withRegistryMaintenanceExecutor(
                        openSpec.executorRegistry().getRegistryMaintenanceExecutor())
                .withChunkStoreCache(chunkStoreCache)
                .build();
    }

    private ChunkStoreCache<K, V> newChunkStoreCache(
            final EffectiveIndexConfiguration<K, V> conf) {
        return new LruChunkStoreCache<>(conf.chunkStoreCache().pageLimit());
    }

    private IndexRetryPolicy newRetryPolicy(
            final EffectiveIndexConfiguration<K, V> conf) {
        return new IndexRetryPolicy(conf.maintenance().busyBackoffMillis(),
                conf.maintenance().busyTimeoutMillis());
    }
}
