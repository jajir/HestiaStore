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

class IndexConfigurationStorageWalTest {

    @Test
    void saveAndLoadRoundTripEnabledWal() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfigurationStorage<String, String> storage = new IndexConfigurationStorage<>(
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
                .identity(identity -> identity.keyClass(String.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(typeDescriptor))//
                .identity(identity -> identity.valueTypeDescriptor(typeDescriptor))//
                .identity(identity -> identity.name("wal-storage-roundtrip"))//
                .logging(logging -> logging.contextEnabled(false))//
                .segment(segment -> segment.cacheKeyLimit(16))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(8))//
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(12))//
                .segment(segment -> segment.chunkKeyLimit(4))//
                .segment(segment -> segment.deltaCacheFileLimit(2))//
                .segment(segment -> segment.maxKeys(32))//
                .segment(segment -> segment.cachedSegmentLimit(8))//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(2))//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(2048))//
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.02D))//
                .io(io -> io.diskBufferSizeBytes(1024))//
                .wal(walBuilder -> walBuilder.configuration(wal))//
                .build();

        storage.save(conf);
        final IndexConfiguration<String, String> loaded = storage.load();

        assertTrue(loaded.wal().isEnabled());
        assertEquals(WalDurabilityMode.SYNC,
                loaded.wal().getDurabilityMode());
        assertEquals(2048L, loaded.wal().getSegmentSizeBytes());
        assertEquals(7, loaded.wal().getGroupSyncDelayMillis());
        assertEquals(512, loaded.wal().getGroupSyncMaxBatchBytes());
        assertEquals(4096L,
                loaded.wal().getMaxBytesBeforeForcedCheckpoint());
        assertEquals(WalCorruptionPolicy.FAIL_FAST,
                loaded.wal().getCorruptionPolicy());
        assertTrue(loaded.wal().isEpochSupport());
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
        final IndexConfigurationStorage<String, String> storage = new IndexConfigurationStorage<>(
                directory);

        final IndexConfiguration<String, String> loaded = storage.load();

        assertSame(Wal.EMPTY, loaded.wal());
        assertFalse(loaded.wal().isEnabled());
    }
}
