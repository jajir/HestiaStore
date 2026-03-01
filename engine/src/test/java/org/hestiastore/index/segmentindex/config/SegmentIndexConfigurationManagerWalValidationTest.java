package org.hestiastore.index.segmentindex.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationBuilder;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.WalReplicationMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexConfigurationManagerWalValidationTest {

    @Mock
    private IndexConfiguratonStorage<Long, String> storage;

    private IndexConfigurationManager<Long, String> manager;

    @BeforeEach
    void setup() {
        manager = new IndexConfigurationManager<>(storage);
    }

    @Test
    void saveRejectsReplicationWhenEpochSupportDisabled() {
        final Wal wal = Wal.builder()//
                .withEnabled(true)//
                .withEpochSupport(false)//
                .withReplicationMode(WalReplicationMode.LEADER)//
                .withSourceNodeId("node-a")//
                .build();
        final IndexConfiguration<Long, String> conf = baseBuilder()//
                .withWal(wal)//
                .build();

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, () -> manager.save(conf));

        assertEquals(
                "Wal epochSupport must be enabled when replication mode is LEADER.",
                ex.getMessage());
    }

    @Test
    void saveRejectsReplicationWithBlankSourceNodeId() {
        final Wal wal = Wal.builder()//
                .withEnabled(true)//
                .withEpochSupport(true)//
                .withReplicationMode(WalReplicationMode.LEADER)//
                .withSourceNodeId("   ")//
                .build();
        final IndexConfiguration<Long, String> conf = baseBuilder()//
                .withWal(wal)//
                .build();

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, () -> manager.save(conf));

        assertEquals(
                "Wal source node id must be non-blank when replication mode is LEADER.",
                ex.getMessage());
    }

    @Test
    void saveAcceptsReplicationWhenEpochSupportAndSourceNodeIdConfigured() {
        final Wal wal = Wal.builder()//
                .withEnabled(true)//
                .withEpochSupport(true)//
                .withReplicationMode(WalReplicationMode.LEADER)//
                .withSourceNodeId("node-a")//
                .build();
        final IndexConfiguration<Long, String> conf = baseBuilder()//
                .withWal(wal)//
                .build();

        manager.save(conf);

        verify(storage).save(any(IndexConfiguration.class));
    }

    private IndexConfigurationBuilder<Long, String> baseBuilder() {
        return IndexConfiguration.<Long, String>builder()//
                .withKeyClass(Long.class)//
                .withValueClass(String.class)//
                .withName("wal-replication-validation")//
                .withKeyTypeDescriptor(TypeDescriptorLong.class.getSimpleName())//
                .withValueTypeDescriptor(
                        TypeDescriptorShortString.class.getSimpleName())//
                .withContextLoggingEnabled(true)//
                .withMaxNumberOfKeysInSegmentCache(12)//
                .withMaxNumberOfKeysInSegmentWriteCache(6)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(8)//
                .withMaxNumberOfKeysInSegmentChunk(4)//
                .withMaxNumberOfDeltaCacheFiles(2)//
                .withMaxNumberOfKeysInSegment(24)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterNumberOfHashFunctions(2)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withIndexWorkerThreadCount(1)//
                .withNumberOfIoThreads(1)//
                .withNumberOfSegmentIndexMaintenanceThreads(1)//
                .withNumberOfIndexMaintenanceThreads(1)//
                .withNumberOfRegistryLifecycleThreads(1)//
                .withIndexBusyBackoffMillis(1)//
                .withIndexBusyTimeoutMillis(1)//
                .withSegmentMaintenanceAutoEnabled(true)//
                .withEncodingFilterClasses(
                        List.of(ChunkFilterCrc32Writing.class))//
                .withDecodingFilterClasses(
                        List.of(ChunkFilterCrc32Validation.class));
    }
}
