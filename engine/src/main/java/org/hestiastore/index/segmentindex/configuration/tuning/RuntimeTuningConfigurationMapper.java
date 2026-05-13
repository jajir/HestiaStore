package org.hestiastore.index.segmentindex.configuration.tuning;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexChunkStoreCacheConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexSegmentConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexWritePathConfiguration;

/**
 * Maps runtime tuning snapshots back into persistable effective configuration.
 */
public final class RuntimeTuningConfigurationMapper {

    private RuntimeTuningConfigurationMapper() {
    }

    public static <K, V> EffectiveIndexConfiguration<K, V> apply(
            final EffectiveIndexConfiguration<K, V> baseConfiguration,
            final RuntimeTuningSnapshot snapshot) {
        final EffectiveIndexConfiguration<K, V> base = Vldtn.requireNonNull(
                baseConfiguration, "baseConfiguration");
        final RuntimeTuningSnapshot tuning = Vldtn.requireNonNull(snapshot,
                "snapshot");
        final EffectiveIndexWritePathConfiguration writePath =
                new EffectiveIndexWritePathConfiguration(
                        tuning.writePath().segmentWriteCacheKeyLimit(),
                        tuning.writePath()
                                .segmentWriteCacheKeyLimitDuringMaintenance(),
                        tuning.writePath().indexBufferedWriteKeyLimit(),
                        tuning.writePath().segmentSplitKeyThreshold());
        final EffectiveIndexSegmentConfiguration segment =
                new EffectiveIndexSegmentConfiguration(base.segment().maxKeys(),
                        base.segment().chunkKeyLimit(),
                        tuning.segment().cacheKeyLimit(),
                        tuning.segment().cachedSegmentLimit(),
                        base.segment().deltaCacheFileLimit());
        final EffectiveIndexChunkStoreCacheConfiguration chunkCache =
                new EffectiveIndexChunkStoreCacheConfiguration(
                        tuning.chunkStoreCache().pageLimit());
        return new EffectiveIndexConfiguration<>(base.identity(), segment,
                writePath, base.bloomFilter(), base.maintenance(), base.io(),
                base.logging(), base.wal(), base.filters(), chunkCache);
    }
}
