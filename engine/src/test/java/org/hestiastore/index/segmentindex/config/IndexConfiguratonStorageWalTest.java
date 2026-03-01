package org.hestiastore.index.segmentindex.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.properties.IndexPropertiesSchema;
import org.hestiastore.index.properties.PropertyStore;
import org.hestiastore.index.properties.PropertyStoreImpl;
import org.hestiastore.index.properties.PropertyTransaction;
import org.hestiastore.index.properties.PropertyWriter;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.WalCorruptionPolicy;
import org.hestiastore.index.segmentindex.WalDurabilityMode;
import org.hestiastore.index.segmentindex.WalReplicationMode;
import org.junit.jupiter.api.Test;

class IndexConfiguratonStorageWalTest {

    @Test
    void saveAndLoadRoundTripEnabledWal() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguratonStorage<String, String> storage = new IndexConfiguratonStorage<>(
                directory);
        final TypeDescriptorShortString typeDescriptor = new TypeDescriptorShortString();
        final Wal wal = Wal.builder()//
                .withEnabled(true)//
                .withDurabilityMode(WalDurabilityMode.SYNC)//
                .withSegmentSizeBytes(2048L)//
                .withGroupSyncDelayMillis(7)//
                .withGroupSyncMaxBatchBytes(512)//
                .withMaxBytesBeforeForcedCheckpoint(4096L)//
                .withCorruptionPolicy(WalCorruptionPolicy.FAIL_FAST)//
                .withEpochSupport(true)//
                .withReplicationMode(WalReplicationMode.LEADER)//
                .withSourceNodeId("node-a")//
                .build();
        final IndexConfiguration<String, String> conf = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(typeDescriptor)//
                .withValueTypeDescriptor(typeDescriptor)//
                .withName("wal-storage-roundtrip")//
                .withContextLoggingEnabled(false)//
                .withMaxNumberOfKeysInSegmentCache(16)//
                .withMaxNumberOfKeysInSegmentWriteCache(8)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(12)//
                .withMaxNumberOfKeysInSegmentChunk(4)//
                .withMaxNumberOfDeltaCacheFiles(2)//
                .withMaxNumberOfKeysInSegment(32)//
                .withMaxNumberOfSegmentsInCache(8)//
                .withBloomFilterNumberOfHashFunctions(2)//
                .withBloomFilterIndexSizeInBytes(2048)//
                .withBloomFilterProbabilityOfFalsePositive(0.02D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withWal(wal)//
                .build();

        storage.save(conf);
        final IndexConfiguration<String, String> loaded = storage.load();

        assertTrue(loaded.getWal().isEnabled());
        assertEquals(WalDurabilityMode.SYNC,
                loaded.getWal().getDurabilityMode());
        assertEquals(2048L, loaded.getWal().getSegmentSizeBytes());
        assertEquals(7, loaded.getWal().getGroupSyncDelayMillis());
        assertEquals(512, loaded.getWal().getGroupSyncMaxBatchBytes());
        assertEquals(4096L,
                loaded.getWal().getMaxBytesBeforeForcedCheckpoint());
        assertEquals(WalCorruptionPolicy.FAIL_FAST,
                loaded.getWal().getCorruptionPolicy());
        assertTrue(loaded.getWal().isEpochSupport());
        assertEquals(WalReplicationMode.LEADER,
                loaded.getWal().getReplicationMode());
        assertEquals("node-a", loaded.getWal().getSourceNodeId());
    }

    @Test
    void loadLegacyConfigurationWithoutWalFieldsDefaultsToWalEmpty() {
        final MemDirectory directory = new MemDirectory();
        final PropertyStore store = PropertyStoreImpl.fromDirectory(directory,
                IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME,
                false);
        try (PropertyTransaction tx = store.beginTransaction()) {
            final PropertyWriter writer = tx.openPropertyWriter();
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_CLASS,
                    String.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_CLASS,
                    String.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_TYPE_DESCRIPTOR,
                    "org.hestiastore.index.datatype.TypeDescriptorString");
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_TYPE_DESCRIPTOR,
                    "org.hestiastore.index.datatype.TypeDescriptorString");
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_INDEX_NAME,
                    "legacy-no-wal");
        }
        final IndexConfiguratonStorage<String, String> storage = new IndexConfiguratonStorage<>(
                directory);

        final IndexConfiguration<String, String> loaded = storage.load();

        assertSame(Wal.EMPTY, loaded.getWal());
        assertFalse(loaded.getWal().isEnabled());
    }

    @Test
    void loadLegacyWalEnabledWithoutReplicationFieldsUsesDefaults() {
        final MemDirectory directory = new MemDirectory();
        final PropertyStore store = PropertyStoreImpl.fromDirectory(directory,
                IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME,
                false);
        try (PropertyTransaction tx = store.beginTransaction()) {
            final PropertyWriter writer = tx.openPropertyWriter();
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_CLASS,
                    String.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_CLASS,
                    String.class.getName());
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_KEY_TYPE_DESCRIPTOR,
                    "org.hestiastore.index.datatype.TypeDescriptorString");
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_VALUE_TYPE_DESCRIPTOR,
                    "org.hestiastore.index.datatype.TypeDescriptorString");
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_INDEX_NAME,
                    "legacy-enabled-wal");
            writer.setBoolean(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_ENABLED,
                    true);
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_DURABILITY_MODE,
                    WalDurabilityMode.GROUP_SYNC.name());
            writer.setLong(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_SEGMENT_SIZE_BYTES,
                    Wal.DEFAULT_SEGMENT_SIZE_BYTES);
            writer.setInt(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_GROUP_SYNC_DELAY_MILLIS,
                    Wal.DEFAULT_GROUP_SYNC_DELAY_MILLIS);
            writer.setInt(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_GROUP_SYNC_MAX_BATCH_BYTES,
                    Wal.DEFAULT_GROUP_SYNC_MAX_BATCH_BYTES);
            writer.setLong(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_MAX_BYTES_BEFORE_FORCED_CHECKPOINT,
                    Wal.DEFAULT_MAX_BYTES_BEFORE_FORCED_CHECKPOINT);
            writer.setString(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_CORRUPTION_POLICY,
                    WalCorruptionPolicy.TRUNCATE_INVALID_TAIL.name());
            writer.setBoolean(
                    IndexPropertiesSchema.IndexConfigurationKeys.PROP_WAL_EPOCH_SUPPORT,
                    false);
        }
        final IndexConfiguratonStorage<String, String> storage = new IndexConfiguratonStorage<>(
                directory);

        final IndexConfiguration<String, String> loaded = storage.load();

        assertTrue(loaded.getWal().isEnabled());
        assertEquals(WalReplicationMode.DISABLED,
                loaded.getWal().getReplicationMode());
        assertEquals("", loaded.getWal().getSourceNodeId());
    }
}
