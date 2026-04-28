package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;

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
import org.hestiastore.index.segmentindex.IndexConfiguration;
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
            extends SegmentIndexImpl<Integer, String> {

        private int startupConsistencyChecks;

        private TrackingIndex(final Directory directoryFacade) {
            this(directoryFacade, buildConf());
        }

        private TrackingIndex(final Directory directoryFacade,
                final IndexConfiguration<Integer, String> conf) {
            super(directoryFacade, new TypeDescriptorInteger(),
                    new TypeDescriptorShortString(), conf,
                    conf.resolveRuntimeConfiguration(),
                    ExecutorRegistryFixture.from(conf));
            completeStartup();
        }

        @Override
        protected void onStartupConsistencyCheck() {
            startupConsistencyChecks++;
        }

        private int getStartupConsistencyChecks() {
            return startupConsistencyChecks;
        }

        private static IndexConfiguration<Integer, String> buildConf() {
            return IndexConfiguration.<Integer, String>builder()
                    .identity(identity -> identity.keyClass(Integer.class))
                    .identity(identity -> identity.valueClass(String.class))
                    .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                    .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                    .identity(identity -> identity.name("segment-index-startup-recovery-test"))
                    .logging(logging -> logging.contextEnabled(false))
                    .segment(segment -> segment.cacheKeyLimit(10))
                    .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                    .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(6))
                    .segment(segment -> segment.chunkKeyLimit(2))
                    .segment(segment -> segment.maxKeys(100))
                    .segment(segment -> segment.cachedSegmentLimit(3))
                    .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                    .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                    .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))
                    .io(io -> io.diskBufferSizeBytes(1024))
                    .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                    .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
                    .build();
        }
    }
}
