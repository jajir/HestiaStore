package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IntegrationIteratorTest extends AbstractSegmentIndexTest {

    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    private Directory directory;
    private SegmentIndex<String, Integer> index;

    private final List<Entry<String, Integer>> indexFile = Arrays.asList(//
            Entry.of("a", 20), //
            Entry.of("b", 30), //
            Entry.of("c", 40));

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        IndexConfiguration<String, Integer> conf = IndexConfiguration
                .<String, Integer>builder().identity(identity -> identity.keyClass(String.class))//
                .identity(identity -> identity.valueClass(Integer.class))//
                .identity(identity -> identity.keyTypeDescriptor(tds)) //
                .identity(identity -> identity.valueTypeDescriptor(tdi)) //
                .segment(segment -> segment.cacheKeyLimit(100)) //
                .segment(segment -> segment.maxKeys(4)) //
                .segment(segment -> segment.chunkKeyLimit(1000)) //
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1000)) //
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(4)) //
                // Keep iterator CRUD tests focused on direct write semantics,
                // not on autonomous split timing introduced by background
                // maintenance.
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(false)) //
                .logging(logging -> logging.contextEnabled(false)) //
                .identity(identity -> identity.name("test_index")) //
                .build();
        index = SegmentIndex.<String, Integer>create(directory, conf);

        writeEntries(index, indexFile);
        index.maintenance().compactAndWait();
    }

    @Test
    void test_case_2_deleted_key() {
        index.delete("b");

        verifyIndexSearch(index, Arrays.asList(//
                Entry.of("a", 20), //
                Entry.of("c", 40) //
        ));
        assertNull(index.get("b"));

        verifyIndexData(index, Arrays.asList(//
                Entry.of("a", 20), //
                Entry.of("c", 40) //
        ));
    }

    @Test
    void test_case_3_modify_key() {
        index.delete("b");
        index.put("e", 28);

        verifyIndexSearch(index, Arrays.asList(//
                Entry.of("a", 20), //
                Entry.of("e", 28), //
                Entry.of("c", 40) //
        ));
        assertNull(index.get("b"));

        verifyIndexData(index, Arrays.asList(//
                Entry.of("a", 20), //
                Entry.of("c", 40), //
                Entry.of("e", 28) //
        ));
    }

    @Test
    void test_case_4_add_key() {
        assertTrue(true);
        index.put("g", 13);

        // verify that added value could be get by key
        verifyIndexSearch(index, Arrays.asList(//
                Entry.of("a", 20), //
                Entry.of("b", 30), //
                Entry.of("c", 40), //
                Entry.of("g", 13) //
        ));

        // verify that added value is in iterator
        verifyIndexData(index, Arrays.asList(//
                Entry.of("a", 20), //
                Entry.of("b", 30), //
                Entry.of("c", 40), //
                Entry.of("g", 13) //
        ));
    }

    @Test
    void test_case_5_flush_make_data_iterable() {
        assertTrue(true);
        index.delete("b");
        index.put("g", 13);
        index.maintenance().flushAndWait();

        // verify data consistency after flush
        verifyIndexSearch(index, Arrays.asList(//
                Entry.of("a", 20), //
                Entry.of("c", 40), //
                Entry.of("g", 13) //
        ));

        // verify that data are in iterator after flush
        verifyIndexData(index, Arrays.asList(//
                Entry.of("a", 20), //
                Entry.of("c", 40), //
                Entry.of("g", 13)//
        ));
    }

    @Test
    void test_basic_consistency() {
        assertTrue(true);
        // verify data consistency after flush
        verifyIndexSearch(index, Arrays.asList(//
                Entry.of("a", 20), //
                Entry.of("b", 30), //
                Entry.of("c", 40) //
        ));

        // verify that data are in iterator after flush
        verifyIndexData(index, Arrays.asList(//
                Entry.of("a", 20), //
                Entry.of("b", 30), //
                Entry.of("c", 40) //
        ));
    }

}
