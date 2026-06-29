package org.hestiastore.index.segmentindex.configuration.tuning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.List;

import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexBloomFilterConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexChunkStoreCacheConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexFilterConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexIdentityConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexIoConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexLoggingConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexMaintenanceConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexSegmentConfiguration;
import org.hestiastore.index.segmentindex.configuration.api.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexWritePathConfiguration;
import org.junit.jupiter.api.Test;

class RuntimeTuningConfigurationMapperTest {

    @Test
    void applyUpdatesRuntimeTunableConfigurationValues() {
        final EffectiveIndexConfiguration<Integer, String> base =
                baseConfiguration();
        final RuntimeTuningSnapshot snapshot = runtimeTuningSnapshot();

        final EffectiveIndexConfiguration<Integer, String> mapped =
                RuntimeTuningConfigurationMapper.apply(base, snapshot);

        assertEquals(1_000, mapped.segment().maxKeys());
        assertEquals(25, mapped.segment().chunkKeyLimit());
        assertEquals(40, mapped.segment().cacheKeyLimit());
        assertEquals(8, mapped.segment().cachedSegmentLimit());
        assertEquals(3, mapped.segment().deltaCacheFileLimit());
        assertEquals(70, mapped.writePath().segmentWriteCacheKeyLimit());
        assertEquals(90, mapped.writePath()
                .segmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(120, mapped.writePath().indexBufferedWriteKeyLimit());
        assertEquals(150, mapped.writePath().segmentSplitKeyThreshold());
        assertEquals(12, mapped.chunkStoreCache().pageLimit());
    }

    @Test
    void applyPreservesNonRuntimeTunableConfigurationValues() {
        final EffectiveIndexConfiguration<Integer, String> base =
                baseConfiguration();
        final RuntimeTuningSnapshot snapshot = runtimeTuningSnapshot();

        final EffectiveIndexConfiguration<Integer, String> mapped =
                RuntimeTuningConfigurationMapper.apply(base, snapshot);

        assertSame(base.identity(), mapped.identity());
        assertSame(base.bloomFilter(), mapped.bloomFilter());
        assertSame(base.maintenance(), mapped.maintenance());
        assertSame(base.io(), mapped.io());
        assertSame(base.logging(), mapped.logging());
        assertSame(base.wal(), mapped.wal());
        assertSame(base.filters(), mapped.filters());
    }

    @Test
    void applyRejectsNullBaseConfiguration() {
        final RuntimeTuningSnapshot snapshot = runtimeTuningSnapshot();

        assertThrows(IllegalArgumentException.class,
                () -> RuntimeTuningConfigurationMapper.apply(null, snapshot));
    }

    @Test
    void applyRejectsNullSnapshot() {
        final EffectiveIndexConfiguration<Integer, String> base =
                baseConfiguration();

        assertThrows(IllegalArgumentException.class,
                () -> RuntimeTuningConfigurationMapper.apply(base, null));
    }

    private EffectiveIndexConfiguration<Integer, String> baseConfiguration() {
        return new EffectiveIndexConfiguration<>(
                new EffectiveIndexIdentityConfiguration<>(
                        "runtime-tuning-mapper-test", Integer.class,
                        String.class,
                        "org.hestiastore.index.datatype.TypeDescriptorInteger",
                        "org.hestiastore.index.datatype.TypeDescriptorString"),
                new EffectiveIndexSegmentConfiguration(1_000, 25, 30, 5, 3),
                new EffectiveIndexWritePathConfiguration(10, 15, 60, 500),
                new EffectiveIndexBloomFilterConfiguration(2, 2_048, 0.01d),
                new EffectiveIndexMaintenanceConfiguration(3, 4, 5, 30,
                        true),
                new EffectiveIndexIoConfiguration(4_096),
                new EffectiveIndexLoggingConfiguration(true),
                IndexWalConfiguration.EMPTY,
                new EffectiveIndexFilterConfiguration(List.of(), List.of()),
                new EffectiveIndexChunkStoreCacheConfiguration(4));
    }

    private RuntimeTuningSnapshot runtimeTuningSnapshot() {
        return new RuntimeTuningSnapshot("runtime-tuning-mapper-test", 7L,
                Instant.now(), new RuntimeSegmentTuningSnapshot(40, 8),
                new RuntimeWritePathTuningSnapshot(70, 90, 120, 150),
                new RuntimeChunkStoreCacheTuningSnapshot(12));
    }
}
