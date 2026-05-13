package org.hestiastore.index.segmentindex.configuration.effective;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfigurationBuilder;
import org.junit.jupiter.api.Test;

class EffectiveIndexConfigurationResolverTest {

    @Test
    void resolveForCreateAppliesDefaultsAndPreservesExplicitFalseValues() {
        final IndexConfiguration<Integer, String> request = baseRequest()
                .maintenance(maintenance -> maintenance
                        .backgroundAutoEnabled(false))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.maxKeys(4)).build();

        final EffectiveIndexConfiguration<Integer, String> effective = EffectiveIndexConfigurationResolver
                .resolveForCreate(request);

        assertEquals("effective-resolver-test", effective.identity().name());
        assertEquals(4, effective.segment().maxKeys());
        assertEquals(4, effective.segment().chunkKeyLimit());
        assertEquals(4,
                effective.writePath().segmentSplitKeyThreshold());
        assertFalse(effective.maintenance().backgroundAutoEnabled());
        assertFalse(effective.logging().contextEnabled());
        assertNotNull(effective.wal());
    }

    @Test
    void mergeWithStoredKeepsStoredFixedValuesAndAppliesRuntimeOverrides() {
        final EffectiveIndexConfiguration<Integer, String> stored = EffectiveIndexConfigurationResolver
                .resolveForCreate(baseRequest().build());
        final IndexConfiguration<Integer, String> request = IndexConfiguration
                .<Integer, String>builder()
                .identity(identity -> identity.name("renamed-index"))
                .segment(segment -> segment.cacheKeyLimit(12))
                .chunkStoreCache(cache -> cache.pageLimit(7))
                .maintenance(maintenance -> maintenance.segmentThreads(2))
                .logging(logging -> logging.contextEnabled(false)).build();

        final EffectiveIndexConfiguration<Integer, String> effective = EffectiveIndexConfigurationResolver
                .mergeWithStored(stored,
                        request);

        assertEquals("renamed-index", effective.identity().name());
        assertEquals(stored.identity().keyClass(),
                effective.identity().keyClass());
        assertEquals(stored.segment().maxKeys(),
                effective.segment().maxKeys());
        assertEquals(12, effective.segment().cacheKeyLimit());
        assertEquals(7, effective.chunkStoreCache().pageLimit());
        assertEquals(2, effective.maintenance().segmentThreads());
        assertFalse(effective.logging().contextEnabled());
    }

    @Test
    void mergeWithStoredRejectsFixedPropertyOverride() {
        final EffectiveIndexConfiguration<Integer, String> stored = EffectiveIndexConfigurationResolver
                .resolveForCreate(baseRequest().build());
        final IndexConfiguration<Integer, String> request = IndexConfiguration
                .<Integer, String>builder()
                .segment(segment -> segment.maxKeys(
                        stored.segment().maxKeys() + 1))
                .build();

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> EffectiveIndexConfigurationResolver
                        .mergeWithStored(stored, request));

        assertEquals(
                "Value of 'MaxNumberOfKeysInSegment' is already set to '1000' and can't be changed to '1001'",
                exception.getMessage());
    }

    private static IndexConfigurationBuilder<Integer, String> baseRequest() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.name("effective-resolver-test"))
                .segment(segment -> segment.maxKeys(1000));
    }
}
