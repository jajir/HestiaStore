package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

class IndexWalConfigurationBuilderValidationTest {

    @Test
    void test_builderUnset_usesDefaults() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder()
                .build();

        assertWalDefaults(wal);
        assertTrue(wal.isEnabled());
    }

    @Test
    void test_durability_setsValue() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder()
                .durability(WalDurabilityMode.SYNC)
                .build();

        assertEquals(WalDurabilityMode.SYNC, wal.getDurabilityMode());
    }

    @Test
    void test_durability_nullUsesDefault() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder().durability(null).build();

        assertEquals(IndexWalConfiguration.DEFAULT_DURABILITY_MODE, wal.getDurabilityMode());
    }

    @Test
    void test_segmentSizeBytes_setsValue() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder().segmentSizeBytes(1024L).build();

        assertEquals(1024L, wal.getSegmentSizeBytes());
    }

    @Test
    void test_segmentSizeBytes_rejectsZero() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> IndexWalConfiguration.builder().segmentSizeBytes(0L).build());

        assertEquals("segmentSizeBytes must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_segmentSizeBytes_rejectsNegative() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> IndexWalConfiguration.builder().segmentSizeBytes(-1L).build());

        assertEquals("segmentSizeBytes must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_groupSyncDelayMillis_setsValue() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder().groupSyncDelayMillis(17).build();

        assertEquals(17, wal.getGroupSyncDelayMillis());
    }

    @Test
    void test_groupSyncDelayMillis_acceptsZero() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder().groupSyncDelayMillis(0).build();

        assertEquals(0, wal.getGroupSyncDelayMillis());
    }

    @Test
    void test_groupSyncDelayMillis_rejectsNegative() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> IndexWalConfiguration.builder().groupSyncDelayMillis(-1).build());

        assertEquals(
                "Property 'groupSyncDelayMillis' must be greater than or equal to 0",
                exception.getMessage());
    }

    @Test
    void test_groupSyncMaxBatchBytes_setsValue() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder().groupSyncMaxBatchBytes(2048)
                .build();

        assertEquals(2048, wal.getGroupSyncMaxBatchBytes());
    }

    @Test
    void test_groupSyncMaxBatchBytes_rejectsZero() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> IndexWalConfiguration.builder().groupSyncMaxBatchBytes(0).build());

        assertEquals(
                "Property 'groupSyncMaxBatchBytes' must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_groupSyncMaxBatchBytes_rejectsNegative() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> IndexWalConfiguration.builder().groupSyncMaxBatchBytes(-1).build());

        assertEquals(
                "Property 'groupSyncMaxBatchBytes' must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_maxBytesBeforeForcedCheckpoint_setsValue() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder()
                .maxBytesBeforeForcedCheckpoint(4096L)
                .build();

        assertEquals(4096L, wal.getMaxBytesBeforeForcedCheckpoint());
    }

    @Test
    void test_maxBytesBeforeForcedCheckpoint_rejectsZero() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> IndexWalConfiguration.builder()
                        .maxBytesBeforeForcedCheckpoint(0L)
                        .build());

        assertEquals("maxBytesBeforeForcedCheckpoint must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_maxBytesBeforeForcedCheckpoint_rejectsNegative() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> IndexWalConfiguration.builder()
                        .maxBytesBeforeForcedCheckpoint(-1L)
                        .build());

        assertEquals("maxBytesBeforeForcedCheckpoint must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_corruptionPolicy_setsValue() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder()
                .corruptionPolicy(WalCorruptionPolicy.FAIL_FAST)
                .build();

        assertEquals(WalCorruptionPolicy.FAIL_FAST,
                wal.getCorruptionPolicy());
    }

    @Test
    void test_corruptionPolicy_nullUsesDefault() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder().corruptionPolicy(null).build();

        assertEquals(IndexWalConfiguration.DEFAULT_CORRUPTION_POLICY,
                wal.getCorruptionPolicy());
    }

    @Test
    void test_epochSupport_setsTrue() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder().epochSupport(true).build();

        assertTrue(wal.isEpochSupport());
    }

    @Test
    void test_epochSupport_setsFalse() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder().epochSupport(false).build();

        assertFalse(wal.isEpochSupport());
    }

    @Test
    void test_walEnabled_usesDefaults() {
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder().wal(IndexWalConfigurationBuilder::enabled)
                .build();

        assertWalDefaults(config.wal());
        assertTrue(config.wal().isEnabled());
    }

    @Test
    void test_walConfiguredWithoutValues_usesDefaults() {
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .wal(wal -> {
                })
                .build();

        assertWalDefaults(config.wal());
        assertTrue(config.wal().isEnabled());
    }

    @Test
    void test_walUnset_keepsEmpty() {
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .build();

        assertSame(IndexWalConfiguration.EMPTY, config.wal());
    }

    @Test
    void test_walDisabled_overridesEarlierEnabledValues() {
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .wal(wal -> wal.segmentSizeBytes(1024L).disabled())
                .build();

        assertSame(IndexWalConfiguration.EMPTY, config.wal());
    }

    @Test
    void test_walDurability_setsValue() {
        final IndexWalConfiguration wal = buildWal(section -> section
                .durability(WalDurabilityMode.SYNC));

        assertEquals(WalDurabilityMode.SYNC, wal.getDurabilityMode());
    }

    @Test
    void test_walDurability_nullUsesDefault() {
        final IndexWalConfiguration wal = buildWal(section -> section.durability(null));

        assertEquals(IndexWalConfiguration.DEFAULT_DURABILITY_MODE, wal.getDurabilityMode());
    }

    @Test
    void test_walSegmentSizeBytes_setsValue() {
        final IndexWalConfiguration wal = buildWal(section -> section.segmentSizeBytes(1024L));

        assertEquals(1024L, wal.getSegmentSizeBytes());
    }

    @Test
    void test_walSegmentSizeBytes_rejectsZero() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> buildWal(section -> section.segmentSizeBytes(0L)));

        assertEquals("segmentSizeBytes must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_walSegmentSizeBytes_rejectsNegative() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> buildWal(section -> section.segmentSizeBytes(-1L)));

        assertEquals("segmentSizeBytes must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_walGroupSyncDelayMillis_setsValue() {
        final IndexWalConfiguration wal = buildWal(section -> section.groupSyncDelayMillis(17));

        assertEquals(17, wal.getGroupSyncDelayMillis());
    }

    @Test
    void test_walGroupSyncDelayMillis_acceptsZero() {
        final IndexWalConfiguration wal = buildWal(section -> section.groupSyncDelayMillis(0));

        assertEquals(0, wal.getGroupSyncDelayMillis());
    }

    @Test
    void test_walGroupSyncDelayMillis_rejectsNegative() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> buildWal(section -> section.groupSyncDelayMillis(-1)));

        assertEquals(
                "Property 'groupSyncDelayMillis' must be greater than or equal to 0",
                exception.getMessage());
    }

    @Test
    void test_walGroupSyncMaxBatchBytes_setsValue() {
        final IndexWalConfiguration wal = buildWal(section -> section
                .groupSyncMaxBatchBytes(2048));

        assertEquals(2048, wal.getGroupSyncMaxBatchBytes());
    }

    @Test
    void test_walGroupSyncMaxBatchBytes_rejectsZero() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> buildWal(section -> section.groupSyncMaxBatchBytes(0)));

        assertEquals(
                "Property 'groupSyncMaxBatchBytes' must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_walGroupSyncMaxBatchBytes_rejectsNegative() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> buildWal(section -> section.groupSyncMaxBatchBytes(-1)));

        assertEquals(
                "Property 'groupSyncMaxBatchBytes' must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_walMaxBytesBeforeForcedCheckpoint_setsValue() {
        final IndexWalConfiguration wal = buildWal(section -> section
                .maxBytesBeforeForcedCheckpoint(4096L));

        assertEquals(4096L, wal.getMaxBytesBeforeForcedCheckpoint());
    }

    @Test
    void test_walMaxBytesBeforeForcedCheckpoint_rejectsZero() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> buildWal(section -> section
                        .maxBytesBeforeForcedCheckpoint(0L)));

        assertEquals("maxBytesBeforeForcedCheckpoint must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_walMaxBytesBeforeForcedCheckpoint_rejectsNegative() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> buildWal(section -> section
                        .maxBytesBeforeForcedCheckpoint(-1L)));

        assertEquals("maxBytesBeforeForcedCheckpoint must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_walCorruptionPolicy_setsValue() {
        final IndexWalConfiguration wal = buildWal(section -> section
                .corruptionPolicy(WalCorruptionPolicy.FAIL_FAST));

        assertEquals(WalCorruptionPolicy.FAIL_FAST,
                wal.getCorruptionPolicy());
    }

    @Test
    void test_walCorruptionPolicy_nullUsesDefault() {
        final IndexWalConfiguration wal = buildWal(section -> section.corruptionPolicy(null));

        assertEquals(IndexWalConfiguration.DEFAULT_CORRUPTION_POLICY,
                wal.getCorruptionPolicy());
    }

    @Test
    void test_walEpochSupport_setsTrue() {
        final IndexWalConfiguration wal = buildWal(section -> section.epochSupport(true));

        assertTrue(wal.isEpochSupport());
    }

    @Test
    void test_walEpochSupport_setsFalse() {
        final IndexWalConfiguration wal = buildWal(section -> section.epochSupport(false));

        assertFalse(wal.isEpochSupport());
    }

    @Test
    void test_walConfiguration_copiesEnabledWal() {
        final IndexWalConfiguration source = IndexWalConfiguration.builder()
                .durability(WalDurabilityMode.SYNC)
                .segmentSizeBytes(1024L)
                .groupSyncDelayMillis(7)
                .groupSyncMaxBatchBytes(2048)
                .maxBytesBeforeForcedCheckpoint(4096L)
                .corruptionPolicy(WalCorruptionPolicy.FAIL_FAST)
                .epochSupport(true)
                .build();

        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .wal(wal -> wal.configuration(source))
                .build();

        assertEquals(source, config.wal());
    }

    @Test
    void test_walConfiguration_emptyDisablesWal() {
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .wal(wal -> wal.segmentSizeBytes(1024L)
                        .configuration(IndexWalConfiguration.EMPTY))
                .build();

        assertSame(IndexWalConfiguration.EMPTY, config.wal());
    }

    @Test
    void test_walConfiguration_nullDisablesWal() {
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .wal(wal -> wal.segmentSizeBytes(1024L)
                        .configuration(null))
                .build();

        assertSame(IndexWalConfiguration.EMPTY, config.wal());
    }

    private static void assertWalDefaults(final IndexWalConfiguration wal) {
        assertEquals(IndexWalConfiguration.DEFAULT_DURABILITY_MODE, wal.getDurabilityMode());
        assertEquals(IndexWalConfiguration.DEFAULT_SEGMENT_SIZE_BYTES,
                wal.getSegmentSizeBytes());
        assertEquals(IndexWalConfiguration.DEFAULT_GROUP_SYNC_DELAY_MILLIS,
                wal.getGroupSyncDelayMillis());
        assertEquals(IndexWalConfiguration.DEFAULT_GROUP_SYNC_MAX_BATCH_BYTES,
                wal.getGroupSyncMaxBatchBytes());
        assertEquals(IndexWalConfiguration.DEFAULT_MAX_BYTES_BEFORE_FORCED_CHECKPOINT,
                wal.getMaxBytesBeforeForcedCheckpoint());
        assertEquals(IndexWalConfiguration.DEFAULT_CORRUPTION_POLICY,
                wal.getCorruptionPolicy());
        assertFalse(wal.isEpochSupport());
    }

    private static IndexWalConfiguration buildWal(
            final Consumer<IndexWalConfigurationBuilder> customizer) {
        return IndexConfiguration.<Integer, String>builder()
                .wal(customizer)
                .build()
                .wal();
    }
}
