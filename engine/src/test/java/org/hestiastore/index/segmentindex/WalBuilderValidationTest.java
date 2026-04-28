package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

class WalBuilderValidationTest {

    @Test
    void test_withDurabilityMode_setsValue() {
        final Wal wal = Wal.builder()
                .withDurabilityMode(WalDurabilityMode.SYNC)
                .build();

        assertEquals(WalDurabilityMode.SYNC, wal.getDurabilityMode());
    }

    @Test
    void test_withDurabilityMode_nullUsesDefault() {
        final Wal wal = Wal.builder().withDurabilityMode(null).build();

        assertEquals(Wal.DEFAULT_DURABILITY_MODE, wal.getDurabilityMode());
    }

    @Test
    void test_withSegmentSizeBytes_setsValue() {
        final Wal wal = Wal.builder().withSegmentSizeBytes(1024L).build();

        assertEquals(1024L, wal.getSegmentSizeBytes());
    }

    @Test
    void test_withSegmentSizeBytes_rejectsZero() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> Wal.builder().withSegmentSizeBytes(0L).build());

        assertEquals("segmentSizeBytes must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_withSegmentSizeBytes_rejectsNegative() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> Wal.builder().withSegmentSizeBytes(-1L).build());

        assertEquals("segmentSizeBytes must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_withGroupSyncDelayMillis_setsValue() {
        final Wal wal = Wal.builder().withGroupSyncDelayMillis(17).build();

        assertEquals(17, wal.getGroupSyncDelayMillis());
    }

    @Test
    void test_withGroupSyncDelayMillis_acceptsZero() {
        final Wal wal = Wal.builder().withGroupSyncDelayMillis(0).build();

        assertEquals(0, wal.getGroupSyncDelayMillis());
    }

    @Test
    void test_withGroupSyncDelayMillis_rejectsNegative() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> Wal.builder().withGroupSyncDelayMillis(-1).build());

        assertEquals(
                "Property 'groupSyncDelayMillis' must be greater than or equal to 0",
                exception.getMessage());
    }

    @Test
    void test_withGroupSyncMaxBatchBytes_setsValue() {
        final Wal wal = Wal.builder().withGroupSyncMaxBatchBytes(2048)
                .build();

        assertEquals(2048, wal.getGroupSyncMaxBatchBytes());
    }

    @Test
    void test_withGroupSyncMaxBatchBytes_rejectsZero() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> Wal.builder().withGroupSyncMaxBatchBytes(0).build());

        assertEquals(
                "Property 'groupSyncMaxBatchBytes' must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_withGroupSyncMaxBatchBytes_rejectsNegative() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> Wal.builder().withGroupSyncMaxBatchBytes(-1).build());

        assertEquals(
                "Property 'groupSyncMaxBatchBytes' must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_withMaxBytesBeforeForcedCheckpoint_setsValue() {
        final Wal wal = Wal.builder()
                .withMaxBytesBeforeForcedCheckpoint(4096L)
                .build();

        assertEquals(4096L, wal.getMaxBytesBeforeForcedCheckpoint());
    }

    @Test
    void test_withMaxBytesBeforeForcedCheckpoint_rejectsZero() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> Wal.builder()
                        .withMaxBytesBeforeForcedCheckpoint(0L)
                        .build());

        assertEquals("maxBytesBeforeForcedCheckpoint must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_withMaxBytesBeforeForcedCheckpoint_rejectsNegative() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> Wal.builder()
                        .withMaxBytesBeforeForcedCheckpoint(-1L)
                        .build());

        assertEquals("maxBytesBeforeForcedCheckpoint must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_withCorruptionPolicy_setsValue() {
        final Wal wal = Wal.builder()
                .withCorruptionPolicy(WalCorruptionPolicy.FAIL_FAST)
                .build();

        assertEquals(WalCorruptionPolicy.FAIL_FAST,
                wal.getCorruptionPolicy());
    }

    @Test
    void test_withCorruptionPolicy_nullUsesDefault() {
        final Wal wal = Wal.builder().withCorruptionPolicy(null).build();

        assertEquals(Wal.DEFAULT_CORRUPTION_POLICY,
                wal.getCorruptionPolicy());
    }

    @Test
    void test_withEpochSupport_setsTrue() {
        final Wal wal = Wal.builder().withEpochSupport(true).build();

        assertTrue(wal.isEpochSupport());
    }

    @Test
    void test_withEpochSupport_setsFalse() {
        final Wal wal = Wal.builder().withEpochSupport(false).build();

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
    void test_walUnset_keepsEmpty() {
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .build();

        assertSame(Wal.EMPTY, config.wal());
    }

    @Test
    void test_walDisabled_overridesEarlierEnabledValues() {
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .wal(wal -> wal.segmentSizeBytes(1024L).disabled())
                .build();

        assertSame(Wal.EMPTY, config.wal());
    }

    @Test
    void test_walDurability_setsValue() {
        final Wal wal = buildWal(section -> section
                .durability(WalDurabilityMode.SYNC));

        assertEquals(WalDurabilityMode.SYNC, wal.getDurabilityMode());
    }

    @Test
    void test_walDurability_nullUsesDefault() {
        final Wal wal = buildWal(section -> section.durability(null));

        assertEquals(Wal.DEFAULT_DURABILITY_MODE, wal.getDurabilityMode());
    }

    @Test
    void test_walSegmentSizeBytes_setsValue() {
        final Wal wal = buildWal(section -> section.segmentSizeBytes(1024L));

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
        final Wal wal = buildWal(section -> section.groupSyncDelayMillis(17));

        assertEquals(17, wal.getGroupSyncDelayMillis());
    }

    @Test
    void test_walGroupSyncDelayMillis_acceptsZero() {
        final Wal wal = buildWal(section -> section.groupSyncDelayMillis(0));

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
        final Wal wal = buildWal(section -> section
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
        final Wal wal = buildWal(section -> section
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
        final Wal wal = buildWal(section -> section
                .corruptionPolicy(WalCorruptionPolicy.FAIL_FAST));

        assertEquals(WalCorruptionPolicy.FAIL_FAST,
                wal.getCorruptionPolicy());
    }

    @Test
    void test_walCorruptionPolicy_nullUsesDefault() {
        final Wal wal = buildWal(section -> section.corruptionPolicy(null));

        assertEquals(Wal.DEFAULT_CORRUPTION_POLICY,
                wal.getCorruptionPolicy());
    }

    @Test
    void test_walEpochSupport_setsTrue() {
        final Wal wal = buildWal(section -> section.epochSupport(true));

        assertTrue(wal.isEpochSupport());
    }

    @Test
    void test_walEpochSupport_setsFalse() {
        final Wal wal = buildWal(section -> section.epochSupport(false));

        assertFalse(wal.isEpochSupport());
    }

    @Test
    void test_walConfiguration_copiesEnabledWal() {
        final Wal source = Wal.builder()
                .withDurabilityMode(WalDurabilityMode.SYNC)
                .withSegmentSizeBytes(1024L)
                .withGroupSyncDelayMillis(7)
                .withGroupSyncMaxBatchBytes(2048)
                .withMaxBytesBeforeForcedCheckpoint(4096L)
                .withCorruptionPolicy(WalCorruptionPolicy.FAIL_FAST)
                .withEpochSupport(true)
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
                        .configuration(Wal.EMPTY))
                .build();

        assertSame(Wal.EMPTY, config.wal());
    }

    @Test
    void test_walConfiguration_nullDisablesWal() {
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .wal(wal -> wal.segmentSizeBytes(1024L)
                        .configuration(null))
                .build();

        assertSame(Wal.EMPTY, config.wal());
    }

    private static void assertWalDefaults(final Wal wal) {
        assertEquals(Wal.DEFAULT_DURABILITY_MODE, wal.getDurabilityMode());
        assertEquals(Wal.DEFAULT_SEGMENT_SIZE_BYTES,
                wal.getSegmentSizeBytes());
        assertEquals(Wal.DEFAULT_GROUP_SYNC_DELAY_MILLIS,
                wal.getGroupSyncDelayMillis());
        assertEquals(Wal.DEFAULT_GROUP_SYNC_MAX_BATCH_BYTES,
                wal.getGroupSyncMaxBatchBytes());
        assertEquals(Wal.DEFAULT_MAX_BYTES_BEFORE_FORCED_CHECKPOINT,
                wal.getMaxBytesBeforeForcedCheckpoint());
        assertEquals(Wal.DEFAULT_CORRUPTION_POLICY,
                wal.getCorruptionPolicy());
        assertFalse(wal.isEpochSupport());
    }

    private static Wal buildWal(
            final Consumer<IndexWalConfigurationBuilder<Integer, String>> customizer) {
        return IndexConfiguration.<Integer, String>builder()
                .wal(customizer)
                .build()
                .wal();
    }
}
