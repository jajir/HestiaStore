package org.hestiastore.index.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningPatch;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningResult;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexRuntimeSnapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 45, unit = TimeUnit.SECONDS,
        threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class SegmentIndexSplitMaterializationIT {

    private static final long SPLIT_TIMEOUT_MILLIS = 30_000L;
    private static final TypeDescriptorInteger TD_INTEGER =
            new TypeDescriptorInteger();
    private static final TypeDescriptorShortString TD_STRING =
            new TypeDescriptorShortString();

    @Test
    void tombstoneHeavySegmentWithTooFewLiveEntriesCompactsAndDoesNotSpin() {
        try (SegmentIndex<Integer, String> index = createIndex(
                new MemDirectory(), "split_tombstone_compact_small")) {
            writeStableKeys(index, 8);
            index.maintenance().compactAndWait();
            awaitSingleReadySegmentWithKeys(index, 8, SPLIT_TIMEOUT_MILLIS);

            final long initialCompactCount =
                    totalSegmentCompactCount(index.runtimeMonitoring()
                            .snapshot());
            for (int key = 0; key < 6; key++) {
                index.delete(key);
            }
            final List<Entry<Integer, String>> expected = entries(6, 8);
            assertFullIsolationSnapshot(index, expected);

            final long scheduledBefore = index.runtimeMonitoring()
                    .snapshot().split().scheduleCount();
            setSplitThreshold(index, 6);

            awaitSplitScheduledAfter(index, scheduledBefore,
                    SPLIT_TIMEOUT_MILLIS);
            awaitSingleReadyCompactedSegment(index, expected.size(),
                    initialCompactCount, SPLIT_TIMEOUT_MILLIS);
            assertFullIsolationSnapshot(index, expected);

            final long settledScheduleCount = index.runtimeMonitoring()
                    .snapshot().split().scheduleCount();
            assertNoAdditionalSplitSchedules(index, settledScheduleCount,
                    1_200L);
            assertEquals(1, index.runtimeMonitoring().snapshot().segments()
                    .count());
        }
    }

    @Test
    void tombstoneHeavySegmentWithEnoughLiveEntriesCompactsThenSplits() {
        try (SegmentIndex<Integer, String> index = createIndex(
                new MemDirectory(), "split_tombstone_compact_then_split")) {
            writeStableKeys(index, 14);
            index.maintenance().compactAndWait();
            awaitSingleReadySegmentWithKeys(index, 14, SPLIT_TIMEOUT_MILLIS);

            final long initialCompactCount =
                    totalSegmentCompactCount(index.runtimeMonitoring()
                            .snapshot());
            for (int key = 0; key < 7; key++) {
                index.delete(key);
            }
            final List<Entry<Integer, String>> expected = entries(7, 14);
            assertFullIsolationSnapshot(index, expected);

            final long scheduledBefore = index.runtimeMonitoring()
                    .snapshot().split().scheduleCount();
            setSplitThreshold(index, 6);

            awaitSingleReadyCompactedSegment(index, expected.size(),
                    initialCompactCount, SPLIT_TIMEOUT_MILLIS);
            awaitSplitPublished(index, scheduledBefore + 1L,
                    SPLIT_TIMEOUT_MILLIS);

            assertFullIsolationSnapshot(index, expected);
            expected.forEach(entry -> assertEquals(entry.getValue(),
                    index.get(entry.getKey())));
            assertTrue(index.runtimeMonitoring().snapshot().split()
                    .scheduleCount() >= scheduledBefore + 2L);
        }
    }

    @Test
    void normalSplitPreservesReadableKeysAndRouteCoverage() {
        try (SegmentIndex<Integer, String> index = createIndex(
                new MemDirectory(), "split_normal_preserves_keys")) {
            final List<Entry<Integer, String>> expected = entries(0, 12);
            writeStableKeys(index, 12);
            index.maintenance().compactAndWait();
            awaitSingleReadySegmentWithKeys(index, 12, SPLIT_TIMEOUT_MILLIS);

            final long scheduledBefore = index.runtimeMonitoring()
                    .snapshot().split().scheduleCount();
            setSplitThreshold(index, 6);
            awaitSplitPublished(index, scheduledBefore, SPLIT_TIMEOUT_MILLIS);

            assertFullIsolationSnapshot(index, expected);
            assertEquals("value-0", index.get(0));
            assertEquals("value-5", index.get(5));
            assertEquals("value-6", index.get(6));
            assertEquals("value-11", index.get(11));
            assertNull(index.get(12));
            assertEquals(12, index.runtimeMonitoring().snapshot().segments()
                    .totalKeys());
        }
    }

    private static SegmentIndex<Integer, String> createIndex(
            final Directory directory, final String name) {
        return SegmentIndex.create(directory, configuration(name));
    }

    private static IndexConfiguration<Integer, String> configuration(
            final String name) {
        return IndexConfiguration
                .<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(TD_INTEGER))
                .identity(identity -> identity.valueTypeDescriptor(TD_STRING))
                .identity(identity -> identity.name(name))
                .segment(segment -> segment.cacheKeyLimit(8))
                .segment(segment -> segment.maxKeys(128))
                .segment(segment -> segment.chunkKeyLimit(4))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(32))
                .writePath(writePath -> writePath
                        .maintenanceWriteCacheKeyLimit(96))
                .writePath(writePath -> writePath
                        .indexBufferedWriteKeyLimit(192))
                .writePath(writePath -> writePath
                        .segmentSplitKeyThreshold(512))
                .bloomFilter(bloomFilter -> bloomFilter
                        .indexSizeBytes(1024 * 128))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(3))
                .maintenance(maintenance -> maintenance
                        .backgroundAutoEnabled(true))
                .build();
    }

    private static void writeStableKeys(
            final SegmentIndex<Integer, String> index, final int count) {
        IntStream.range(0, count)
                .forEach(key -> index.put(key, "value-" + key));
    }

    private static List<Entry<Integer, String>> entries(
            final int fromInclusive, final int toExclusive) {
        return IntStream.range(fromInclusive, toExclusive)
                .mapToObj(key -> Entry.of(key, "value-" + key))
                .toList();
    }

    private static void setSplitThreshold(
            final SegmentIndex<Integer, String> index, final int threshold) {
        final long revision = index.runtimeTuning().current().revision();
        final RuntimeTuningResult patchResult = index.runtimeTuning()
                .apply(RuntimeTuningPatch.builder()
                        .expectedRevision(revision)
                        .segmentSplitKeyThreshold(threshold)
                        .build());
        assertTrue(patchResult.applied());
    }

    private static void awaitSplitScheduledAfter(
            final SegmentIndex<Integer, String> index,
            final long scheduledBefore, final long timeoutMillis) {
        awaitCondition(() -> index.runtimeMonitoring().snapshot().split()
                .scheduleCount() > scheduledBefore, timeoutMillis);
    }

    private static void awaitSingleReadySegmentWithKeys(
            final SegmentIndex<Integer, String> index, final long expectedKeys,
            final long timeoutMillis) {
        awaitCondition(() -> {
            final SegmentIndexRuntimeSnapshot snapshot = index
                    .runtimeMonitoring().snapshot();
            return splitIdle(snapshot)
                    && snapshot.segments().count() == 1
                    && snapshot.segments().readyCount() == 1
                    && snapshot.segments().totalKeys() == expectedKeys;
        }, timeoutMillis);
    }

    private static void awaitSingleReadyCompactedSegment(
            final SegmentIndex<Integer, String> index, final long expectedKeys,
            final long initialCompactCount, final long timeoutMillis) {
        awaitCondition(() -> {
            final SegmentIndexRuntimeSnapshot snapshot = index
                    .runtimeMonitoring().snapshot();
            return splitIdle(snapshot)
                    && snapshot.segments().count() == 1
                    && snapshot.segments().readyCount() == 1
                    && snapshot.segments().totalKeys() == expectedKeys
                    && snapshot.segments().totalCacheKeys() == 0L
                    && snapshot.writePath().totalBufferedWriteKeys() == 0L
                    && totalSegmentCompactCount(snapshot) > initialCompactCount;
        }, timeoutMillis);
    }

    private static void awaitSplitPublished(
            final SegmentIndex<Integer, String> index,
            final long scheduledBefore, final long timeoutMillis) {
        awaitCondition(() -> {
            final SegmentIndexRuntimeSnapshot snapshot = index
                    .runtimeMonitoring().snapshot();
            return snapshot.segments().count() > 1
                    && splitIdle(snapshot)
                    && snapshot.split().scheduleCount() > scheduledBefore;
        }, timeoutMillis);
    }

    private static void assertNoAdditionalSplitSchedules(
            final SegmentIndex<Integer, String> index,
            final long expectedScheduleCount, final long quietMillis) {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(quietMillis);
        while (System.nanoTime() < deadline) {
            assertEquals(expectedScheduleCount,
                    index.runtimeMonitoring().snapshot().split()
                            .scheduleCount());
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20L));
        }
        assertEquals(expectedScheduleCount,
                index.runtimeMonitoring().snapshot().split().scheduleCount());
    }

    private static void assertFullIsolationSnapshot(
            final SegmentIndex<Integer, String> index,
            final List<Entry<Integer, String>> expected) {
        try (var stream = index.getStream(SegmentWindow.unbounded(),
                SegmentIteratorIsolation.FULL_ISOLATION)) {
            assertEquals(expected, stream.toList());
        }
    }

    private static boolean splitIdle(
            final SegmentIndexRuntimeSnapshot snapshot) {
        return snapshot.split().inFlightCount() == 0
                && snapshot.split().executor().queueSize() == 0
                && snapshot.split().executor().activeThreadCount() == 0;
    }

    private static long totalSegmentCompactCount(
            final SegmentIndexRuntimeSnapshot snapshot) {
        return snapshot.segments().runtimeMetrics().stream()
                .mapToLong(metric -> metric.compactRequestCount())
                .sum();
    }

    private static void awaitCondition(final Supplier<Boolean> condition,
            final long timeoutMillis) {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.get()) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20L));
            if (Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting");
            }
        }
        assertTrue(condition.get(),
                "Condition not reached within " + timeoutMillis + " ms.");
    }
}
