package org.hestiastore.index.segmentindex;

import static org.hestiastore.index.datatype.NullValue.NULL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IntegrationSegmentIndexIteratorTest {

    private static final long SPLIT_REMAPPING_TIMEOUT_MILLIS = 30_000L;
    private static final TypeDescriptorShortString TD_STRING = new TypeDescriptorShortString();
    private static final TypeDescriptorInteger TD_INTEGER = new TypeDescriptorInteger();

    private final Logger logger = LoggerFactory
            .getLogger(IntegrationSegmentIndexIteratorTest.class);

    private final Directory directory = new MemDirectory();
    private final List<Entry<Integer, String>> data = List.of(
            Entry.of(1, "bbb"), Entry.of(2, "ccc"), Entry.of(3, "dde"),
            Entry.of(4, "ddf"), Entry.of(5, "ddg"), Entry.of(6, "ddh"),
            Entry.of(7, "ddi"), Entry.of(8, "ddj"), Entry.of(9, "ddk"),
            Entry.of(10, "ddl"), Entry.of(11, "ddm"));
    private final List<Entry<Integer, NullValue>> data2 = List.of(
            Entry.of(1, NULL), Entry.of(2, NULL), Entry.of(3, NULL),
            Entry.of(4, NULL), Entry.of(5, NULL), Entry.of(6, NULL),
            Entry.of(7, NULL), Entry.of(8, NULL), Entry.of(9, NULL),
            Entry.of(10, NULL), Entry.of(11, NULL));

    @Test
    void test_simple_index_building() {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withName("test_index")//
                .build();
        final SegmentIndex<Integer, String> index = SegmentIndex.create(directory, conf);
        data.stream().forEach(index::put);
        index.compact();
        assertTrue(true); // Just to ensure no exceptions are thrown
    }

    @Test
    void test_null_value() {
        final IndexConfiguration<Integer, NullValue> conf = IndexConfiguration
                .<Integer, NullValue>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(NullValue.class)//
                .withName("test_index")//
                .build();
        final SegmentIndex<Integer, NullValue> index = SegmentIndex.create(directory, conf);
        data2.stream().forEach(index::put);
        index.compact();
        assertTrue(true); // Just to ensure no exceptions are thrown
    }

    @Test
    void test_string_defaults() {
        final IndexConfiguration<String, String> conf = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withName("test_index")//
                .build();
        final SegmentIndex<String, String> index = SegmentIndex.create(directory, conf);
        index.put("a", "a");
        index.put("b", "b");
        index.compact();
        assertTrue(true); // Just to ensure no exceptions are thrown
    }
    // TEST nkey class non existing conf

    @Test
    void testBasic() {
        final SegmentIndex<Integer, String> index1 = makeSegmentIndex();

        data.stream().forEach(index1::put);
        index1.compactAndWait();
        logger.debug("verify that after that point no segment "
                + "is loaded into memory.");
        index1.getStream(SegmentWindow.unbounded()).forEach(entry -> {
            assertTrue(data.contains(entry));
        });

        assertEquals(data.size(),
                index1.getStream(SegmentWindow.unbounded()).count());
    }

    @Test
    void fullIsolationStreamMergesStableSegmentWithActiveOverlayAndFiltersTombstones() {
        try (SegmentIndex<Integer, String> index = makeSegmentIndex()) {
            index.put(1, "stable-1");
            index.put(2, "stable-2");
            index.put(3, "stable-3");
            index.put(4, "stable-4");
            index.flushAndWait();
            index.compactAndWait();

            index.delete(1);
            index.put(2, "overlay-2");
            index.delete(4);
            index.put(5, "overlay-5");

            final List<Entry<Integer, String>> visible;
            try (var stream = index.getStream(SegmentWindow.unbounded(),
                    SegmentIteratorIsolation.FULL_ISOLATION)) {
                visible = stream.toList();
            }

            assertEquals(List.of(Entry.of(2, "overlay-2"),
                    Entry.of(3, "stable-3"), Entry.of(5, "overlay-5")),
                    visible);
            assertNull(index.get(1));
            assertEquals("overlay-2", index.get(2));
            assertEquals("stable-3", index.get(3));
            assertNull(index.get(4));
            assertEquals("overlay-5", index.get(5));
        }
    }

    @Test
    void fullIsolationStreamKeepsSnapshotOpenedBeforeLaterWrites() {
        try (SegmentIndex<Integer, String> index = makeSegmentIndex()) {
            index.put(1, "stable-1");
            index.put(2, "stable-2");
            index.flushAndWait();
            index.compactAndWait();
            index.put(3, "overlay-before-open");

            final List<Entry<Integer, String>> snapshotView;
            try (var stream = index.getStream(SegmentWindow.unbounded(),
                    SegmentIteratorIsolation.FULL_ISOLATION)) {
                index.put(2, "overlay-after-open");
                index.delete(1);
                index.put(4, "overlay-after-open");

                assertNull(index.get(1));
                assertEquals("overlay-after-open", index.get(2));
                assertEquals("overlay-before-open", index.get(3));
                assertEquals("overlay-after-open", index.get(4));

                snapshotView = stream.toList();
            }

            assertEquals(List.of(Entry.of(1, "stable-1"),
                    Entry.of(2, "stable-2"),
                    Entry.of(3, "overlay-before-open")), snapshotView);

            try (var stream = index.getStream(SegmentWindow.unbounded(),
                    SegmentIteratorIsolation.FULL_ISOLATION)) {
                assertEquals(List.of(Entry.of(2, "overlay-after-open"),
                        Entry.of(3, "overlay-before-open"),
                        Entry.of(4, "overlay-after-open")), stream.toList());
            }
        }
    }

    @Test
    void fullIsolationStreamsRemainConsistentWhileSplitReassignsOverlayToChildRoutes() {
        try (SegmentIndex<Integer, String> index = makeAutonomousSplitIndex()) {
            for (int i = 0; i < 48; i++) {
                index.put(i, "stable-" + i);
            }
            index.flushAndWait();
            awaitCondition(() -> index.metricsSnapshot().getSegmentCount() == 1
                    && index.metricsSnapshot().getSplitInFlightCount() == 0
                    && index.metricsSnapshot().getDrainInFlightCount() == 0,
                    10_000L);

            index.put(5, "overlay-5");
            index.delete(18);
            index.put(44, "overlay-44");
            index.put(49, "overlay-49");

            final List<Entry<Integer, String>> expected = IntStream
                    .concat(IntStream.range(0, 48), IntStream.of(49))
                    .filter(key -> key != 18)
                    .mapToObj(key -> {
                        if (key == 5) {
                            return Entry.of(key, "overlay-5");
                        }
                        if (key == 44) {
                            return Entry.of(key, "overlay-44");
                        }
                        if (key == 49) {
                            return Entry.of(key, "overlay-49");
                        }
                        return Entry.of(key, "stable-" + key);
                    }).toList();

            try (var preSplitStream = index.getStream(SegmentWindow.unbounded(),
                    SegmentIteratorIsolation.FULL_ISOLATION)) {
                final long revision = index.controlPlane().configuration()
                        .getConfigurationActual().getRevision();
                final RuntimePatchResult patchResult = index.controlPlane()
                        .configuration()
                        .apply(new RuntimeConfigPatch(Map.of(
                                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
                                Integer.valueOf(16)), false,
                                Long.valueOf(revision)));
                assertTrue(patchResult.isApplied());

                awaitCondition(() -> {
                    assertFullIsolationSnapshot(index, expected);
                    final SegmentIndexMetricsSnapshot snapshot = index
                            .metricsSnapshot();
                    return snapshot.getSegmentCount() > 1
                            && snapshot.getSplitInFlightCount() == 0;
                }, SPLIT_REMAPPING_TIMEOUT_MILLIS);

                assertEquals(expected, preSplitStream.toList());
            }

            assertEquals("overlay-5", index.get(5));
            assertNull(index.get(18));
            assertEquals("overlay-44", index.get(44));
            assertEquals("overlay-49", index.get(49));
            assertFullIsolationSnapshot(index, expected);
            assertTrue(index.metricsSnapshot().getSegmentCount() > 1);
        }
    }

    @Test
    void failFastStreamOpenedBeforeSplitRemapKeepsStablePrefix() {
        try (SegmentIndex<Integer, String> index = makeAutonomousSplitIndex()) {
            for (int i = 0; i < 48; i++) {
                index.put(i, "stable-" + i);
            }
            index.flushAndWait();
            awaitCondition(() -> index.metricsSnapshot().getSegmentCount() == 1
                    && index.metricsSnapshot().getSplitInFlightCount() == 0
                    && index.metricsSnapshot().getDrainInFlightCount() == 0,
                    10_000L);

            try (var stream = index.getStream(SegmentWindow.unbounded(),
                    SegmentIteratorIsolation.FAIL_FAST)) {
                final var iterator = stream.iterator();
                final List<Entry<Integer, String>> consumed = new ArrayList<>();
                assertTrue(iterator.hasNext());
                consumed.add(iterator.next());

                final long revision = index.controlPlane().configuration()
                        .getConfigurationActual().getRevision();
                final RuntimePatchResult patchResult = index.controlPlane()
                        .configuration()
                        .apply(new RuntimeConfigPatch(Map.of(
                                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
                                Integer.valueOf(16)), false,
                                Long.valueOf(revision)));
                assertTrue(patchResult.isApplied());

                awaitCondition(() -> {
                    final SegmentIndexMetricsSnapshot snapshot = index
                            .metricsSnapshot();
                    return snapshot.getSegmentCount() > 1
                            && snapshot.getSplitInFlightCount() == 0
                            && snapshot.getDrainInFlightCount() == 0;
                }, SPLIT_REMAPPING_TIMEOUT_MILLIS);

                while (iterator.hasNext()) {
                    consumed.add(iterator.next());
                }

                assertTrue(consumed.size() > 0);
                assertEquals(IntStream.range(0, consumed.size())
                        .mapToObj(key -> Entry.of(key, "stable-" + key))
                        .toList(), consumed);
            }
        }
    }

    private SegmentIndex<Integer, String> makeSegmentIndex() {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_INTEGER) //
                .withValueTypeDescriptor(TD_STRING) //
                .withMaxNumberOfKeysInSegmentCache(3) //
                .withMaxNumberOfKeysInActivePartition(64) //
                .withMaxNumberOfKeysInPartitionBuffer(128) //
                .withMaxNumberOfKeysInIndexBuffer(256) //
                .withMaxNumberOfKeysInPartitionBeforeSplit(512) //
                .withMaxNumberOfKeysInSegment(4) //
                .withMaxNumberOfKeysInSegmentChunk(1) //
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(4) //
                .withDiskIoBufferSizeInBytes(1024)//
                .withBackgroundMaintenanceAutoEnabled(false) //
                .withName("test_index")//
                .build();
        return SegmentIndex.<Integer, String>create(directory, conf);
    }

    private SegmentIndex<Integer, String> makeAutonomousSplitIndex() {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_INTEGER) //
                .withValueTypeDescriptor(TD_STRING) //
                .withMaxNumberOfKeysInSegmentCache(8) //
                .withMaxNumberOfKeysInActivePartition(32) //
                .withMaxNumberOfImmutableRunsPerPartition(2) //
                .withMaxNumberOfKeysInPartitionBuffer(96) //
                .withMaxNumberOfKeysInIndexBuffer(192) //
                .withMaxNumberOfKeysInPartitionBeforeSplit(512) //
                .withMaxNumberOfKeysInSegment(128) //
                .withMaxNumberOfKeysInSegmentChunk(4) //
                .withBloomFilterIndexSizeInBytes(1024 * 128) //
                .withBloomFilterNumberOfHashFunctions(3) //
                .withBackgroundMaintenanceAutoEnabled(true) //
                .withName("test_index_autonomous_split")//
                .build();
        return SegmentIndex.<Integer, String>create(directory, conf);
    }

    private static void assertFullIsolationSnapshot(
            final SegmentIndex<Integer, String> index,
            final List<Entry<Integer, String>> expected) {
        try (var stream = index.getStream(SegmentWindow.unbounded(),
                SegmentIteratorIsolation.FULL_ISOLATION)) {
            assertEquals(expected, stream.toList());
        }
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
                throw new AssertionError("Interrupted while waiting");
            }
        }
        assertTrue(condition.get(),
                "Condition not reached within " + timeoutMillis + " ms.");
    }

}
