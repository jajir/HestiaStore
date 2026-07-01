package org.hestiastore.index.it;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class SegmentIndexManualMaintenanceIT {

    private static final int WRITE_CACHE_LIMIT = 4;
    private static final int COPY_OFFSET = 100;

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void streamWriteWithBackgroundMaintenanceDisabledFailsWhenWriteCacheFull() {
        final Directory directory = new MemDirectory();
        try (SegmentIndex<Integer, String> index = SegmentIndex.create(
                directory, configuration())) {
            for (int key = 0; key < WRITE_CACHE_LIMIT; key++) {
                index.put(key, "value-" + key);
            }

            final IndexException exception = assertThrows(IndexException.class,
                    () -> copyFirstEntry(index));
            assertTrue(exception.getMessage().contains("Write cache is full"));
        }
    }

    private static void copyFirstEntry(
            final SegmentIndex<Integer, String> index) {
        try (Stream<Entry<Integer, String>> stream = index.getStream()) {
            stream.limit(1).forEach(entry -> index.put(
                    entry.getKey() + COPY_OFFSET, entry.getValue()));
        }
    }

    private static IndexConfiguration<Integer, String> configuration() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class)
                        .valueClass(String.class)
                        .name("manual-maintenance-stream-write"))
                .segment(segment -> segment.maxKeys(128).cacheKeyLimit(64)
                        .cachedSegmentLimit(4).chunkKeyLimit(4)
                        .deltaCacheFileLimit(64))
                .writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(WRITE_CACHE_LIMIT)
                        .maintenanceWriteCacheKeyLimit(WRITE_CACHE_LIMIT * 2)
                        .indexBufferedWriteKeyLimit(WRITE_CACHE_LIMIT * 4)
                        .segmentSplitKeyThreshold(128))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024)
                        .hashFunctions(3).falsePositiveProbability(0.01D))
                .maintenance(maintenance -> maintenance
                        .backgroundAutoEnabled(false).busyBackoffMillis(1)
                        .busyTimeoutMillis(60_000))
                .logging(logging -> logging.contextEnabled(false))
                .build();
    }
}
