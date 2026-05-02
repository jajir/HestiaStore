package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Basic index integrations tests.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS,
        threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class IntegrationSegmentIndexTest extends AbstractSegmentIndexTest {
    final Directory directory = new MemDirectory();
    final SegmentId id = SegmentId.of(27);
    final TypeDescriptorShortString tds = new TypeDescriptorShortString();
    final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    private final List<Entry<Integer, String>> testData = List.of(
            Entry.of(1, "bbb"), Entry.of(2, "ccc"), Entry.of(3, "dde"),
            Entry.of(4, "ddf"), Entry.of(5, "ddg"), Entry.of(6, "ddh"),
            Entry.of(7, "ddi"), Entry.of(8, "ddj"), Entry.of(9, "ddk"),
            Entry.of(10, "ddl"), Entry.of(11, "ddm"));

    @Test
    void testBasic() {
        assertTrue(true);
        final SegmentIndex<Integer, String> index = makeSegmentIndex(false);
        writeEntries(index, testData);

        /**
         * Calling of verifyIndexData before compact() will fail. It's by
         * design.
         */

        verifyIndexSearch(index, testData);
        index.maintenance().compactAndWait();

        verifyIndexData(index, testData);
        verifyIndexSearch(index, testData);

        index.close();
    }

    @Test
    void test_duplicated_operations() {
        assertTrue(true);
        final SegmentIndex<Integer, String> index = makeSegmentIndex(false);
        for (int i = 0; i < 100; i++) {
            index.put(i, "kachna");
            index.delete(i);
        }
        index.maintenance().compactAndWait();
        verifyIndexData(index, new ArrayList<>());
    }

    @Test
    void test_delete_search_operations() {
        final SegmentIndex<Integer, String> index = makeSegmentIndex(false);
        for (int i = 0; i < 64; i++) {
            index.put(i, "kachna");
            assertEquals("kachna", index.get(i));
            index.delete(i);
            assertNull(index.get(i));
            awaitMaintenanceIdle(index);
            verifyIndexData(index, List.of());
        }
        awaitMaintenanceIdle(index);
        verifyIndexData(index, List.of());
    }

    /**
     * In this test getStream() could ommit some results
     * 
     * @param iterations @
     */
    @ParameterizedTest
    @CsvSource(value = { "1:1", "3:3", "5:5", "15:15", "100:100",
            "102:102" }, delimiter = ':')
    void test_adds_and_deletes_operations_no_compacting(final int iterations,
            final int itemsInIndex) {
        final SegmentIndex<Integer, String> index = makeSegmentIndex(false);
        for (int i = 0; i < iterations; i++) {
            index.put(i, "kachna");
            assertEquals("kachna", index.get(i));
        }
        assertEquals(itemsInIndex,
                index.getStream(SegmentWindow.unbounded()).count());
        for (int i = 0; i < iterations; i++) {
            index.delete(i);
            assertNull(index.get(i));
        }
        verifyIndexData(index, List.of());
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 3, 5, 15, 100, 102 })
    void test_adds_and_deletes_operations_with_compacting(
            final int iterations) {
        final SegmentIndex<Integer, String> index = makeSegmentIndex(false);
        for (int i = 0; i < iterations; i++) {
            index.put(i, "kachna");
            assertEquals("kachna", index.get(i));
        }
        index.maintenance().compactAndWait();
        assertEquals(iterations,
                index.getStream(SegmentWindow.unbounded()).count());
        for (int i = 0; i < iterations; i++) {
            index.delete(i);
            assertNull(index.get(i));
        }
        index.maintenance().compactAndWait();
        verifyIndexData(index, List.of());
    }

    private SegmentIndex<Integer, String> makeSegmentIndex(boolean withLog) {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(tdi)) //
                .identity(identity -> identity.valueTypeDescriptor(tds)) //
                .segment(segment -> segment.cacheKeyLimit(16)) //
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(128)) //
                .writePath(writePath -> writePath.legacyImmutableRunLimit(4)) //
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(256)) //
                .writePath(writePath -> writePath.indexBufferedWriteKeyLimit(512)) //
                .writePath(writePath -> writePath.segmentSplitKeyThreshold(256)) //
                // Keep CRUD integrations focused on index semantics, not on
                // stable-segment split pressure introduced by direct writes.
                .segment(segment -> segment.maxKeys(1_024)) //
                .segment(segment -> segment.chunkKeyLimit(2)) //
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1000)) //
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(3)) //
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(false)) //
                .logging(logging -> logging.contextEnabled(withLog)) //
                .identity(identity -> identity.name("test_index")) //
                .build();
        return SegmentIndex.create(directory, conf);
    }

}
