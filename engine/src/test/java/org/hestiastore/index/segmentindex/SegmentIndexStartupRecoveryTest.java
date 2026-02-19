package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileWriter;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SegmentIndexStartupRecoveryTest {

    @Test
    void startupRunsConsistencyCheckWhenStaleLockWasRecovered() {
        final MemDirectory directory = new MemDirectory();
        writeStaleLock(directory);

        final TrackingIndex index = new TrackingIndex(directory);
        try {
            assertEquals(1, index.getStartupConsistencyChecks());
        } finally {
            index.close();
        }
    }

    @Test
    void startupSkipsConsistencyCheckWithoutStaleLockRecovery() {
        final TrackingIndex index = new TrackingIndex(new MemDirectory());
        try {
            assertEquals(0, index.getStartupConsistencyChecks());
        } finally {
            index.close();
        }
    }

    private static void writeStaleLock(final Directory directory) {
        final String host = resolveHostName();
        final String lockContent = "version=1\n" + "pid=" + Long.MAX_VALUE
                + "\n" + "processStartEpochMillis=1\n" + "host=" + host + "\n"
                + "sessionId=stale-lock\n";
        try (FileWriter writer = directory.getFileWriter(".lock")) {
            writer.write(lockContent.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String resolveHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException e) {
            return "unknown-host";
        }
    }

    private static final class TrackingIndex
            extends IndexInternalConcurrent<Integer, String> {

        private int startupConsistencyChecks;

        private TrackingIndex(final Directory directoryFacade) {
            super(directoryFacade, new TypeDescriptorInteger(),
                    new TypeDescriptorShortString(), buildConf());
        }

        @Override
        public void checkAndRepairConsistency() {
            startupConsistencyChecks++;
            super.checkAndRepairConsistency();
        }

        private int getStartupConsistencyChecks() {
            return startupConsistencyChecks;
        }

        private static IndexConfiguration<Integer, String> buildConf() {
            return IndexConfiguration.<Integer, String>builder()
                    .withKeyClass(Integer.class)
                    .withValueClass(String.class)
                    .withKeyTypeDescriptor(new TypeDescriptorInteger())
                    .withValueTypeDescriptor(new TypeDescriptorShortString())
                    .withName("segment-index-startup-recovery-test")
                    .withContextLoggingEnabled(false)
                    .withMaxNumberOfKeysInSegmentCache(10)
                    .withMaxNumberOfKeysInSegmentWriteCache(5)
                    .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(6)
                    .withMaxNumberOfKeysInSegmentChunk(2)
                    .withMaxNumberOfKeysInSegment(100)
                    .withMaxNumberOfSegmentsInCache(3)
                    .withBloomFilterNumberOfHashFunctions(1)
                    .withBloomFilterIndexSizeInBytes(1024)
                    .withBloomFilterProbabilityOfFalsePositive(0.01D)
                    .withDiskIoBufferSizeInBytes(1024)
                    .withIndexWorkerThreadCount(1)
                    .withNumberOfIoThreads(1)
                    .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                    .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                    .build();
        }
    }
}
