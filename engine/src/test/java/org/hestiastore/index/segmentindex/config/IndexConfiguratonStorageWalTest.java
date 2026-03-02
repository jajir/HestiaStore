package org.hestiastore.index.segmentindex.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.properties.IndexPropertiesSchema;
import org.hestiastore.index.properties.PropertyStore;
import org.hestiastore.index.properties.PropertyStoreImpl;
import org.hestiastore.index.properties.PropertyTransaction;
import org.hestiastore.index.properties.PropertyWriter;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.WalCorruptionPolicy;
import org.hestiastore.index.segmentindex.WalDurabilityMode;
import org.junit.jupiter.api.Test;

class IndexConfiguratonStorageWalTest {

    @Test
    void saveAndLoadRoundTripEnabledWal() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguratonStorage<String, String> storage = new IndexConfiguratonStorage<>(
                directory);
        final TypeDescriptorShortString typeDescriptor = new TypeDescriptorShortString();
        final Wal wal = Wal.builder()//
                .withDurabilityMode(WalDurabilityMode.SYNC)//
                .withSegmentSizeBytes(2048L)//
                .withGroupSyncDelayMillis(7)//
                .withGroupSyncMaxBatchBytes(512)//
                .withMaxBytesBeforeForcedCheckpoint(4096L)//
                .withCorruptionPolicy(WalCorruptionPolicy.FAIL_FAST)//
                .withEpochSupport(true)//
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
}
