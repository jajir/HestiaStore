package org.hestiastore.index.scarceindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class IntegrationScarceIndexTest {

    private static final String FILE_NAME = "pok.dat";
    private final TypeDescriptorShortString stringTd = new TypeDescriptorShortString();

    private static final Entry<String, Integer> P_BBB_1 = Entry.of("bbb", 13);
    private static final Entry<String, Integer> P_BBB_2 = Entry.of("bbb", 1);
    private static final Entry<String, Integer> P_CCC_1 = Entry.of("ccc", 2);
    private static final Entry<String, Integer> P_CCC_2 = Entry.of("ccc", 3);
    private static final Entry<String, Integer> P_DDD = Entry.of("ddd", 3);
    private static final Entry<String, Integer> P_EEE = Entry.of("eee", 4);
    private static final Entry<String, Integer> P_FFF_1 = Entry.of("fff", 4);
    private static final Entry<String, Integer> P_FFF_2 = Entry.of("fff", 5);

    @Test
    void test_one_key() {
        final ScarceSegmentIndex<String> index = makeIndex(List.of(P_BBB_1));

        assertEquals(13, index.get("bbb"));
        assertNull(index.get("ddd"));
        assertNull(index.get("ccc"));

        assertEquals("bbb", index.getMinKey());
        assertEquals("bbb", index.getMaxKey());
    }

    @Test
    void test_empty() {
        final ScarceSegmentIndex<String> index = makeIndex(
                Collections.emptyList());

        assertNull(index.get("aaa"));
        assertNull(index.get("bbb"));
        assertNull(index.get("ccc"));

        assertNull(index.getMinKey());
        assertNull(index.getMaxKey());
    }

    @Test
    void test_one_multiple() {
        final ScarceSegmentIndex<String> index = makeIndex(
                List.of(P_BBB_2, P_CCC_1, P_DDD, P_EEE, P_FFF_2));

        assertEquals(1, index.get("aaa"));
        assertEquals(1, index.get("bbb"));
        assertEquals(2, index.get("ccc"));
        assertEquals(3, index.get("ccd"));
        assertEquals(3, index.get("cee"));
        assertEquals(5, index.get("fff"));
        assertNull(index.get("ggg"));
        assertEquals("bbb", index.getMinKey());
        assertEquals("fff", index.getMaxKey());
    }

    @Test
    void test_insert_duplicite_keys() {
        final List<Entry<String, Integer>> entries = List.of(P_BBB_2, P_CCC_1,
                P_CCC_2, P_EEE, P_FFF_2);
        assertThrows(IllegalArgumentException.class, () -> makeIndex(entries));
    }

    @Test
    void test_sanity_check() {
        final List<Entry<String, Integer>> entries = List.of(P_BBB_2, P_CCC_1,
                P_DDD, P_EEE, P_FFF_1);
        assertThrows(IllegalStateException.class, () -> makeIndex(entries));
    }

    @Test
    void test_overwrite_index() {
        final ScarceSegmentIndex<String> index = makeIndex(
                List.of(P_BBB_2, P_CCC_1, P_DDD, P_EEE, P_FFF_2));

        index.openWriterTx().execute(writer -> {
            writer.write(P_BBB_2);
        });

        assertEquals(1, index.get("aaa"));
        assertEquals(1, index.get("bbb"));
        assertNull(index.get("ccc"));
    }

    private ScarceSegmentIndex<String> makeIndex(
            final List<Entry<String, Integer>> entries) {
        final MemDirectory directory = new MemDirectory();
        final ScarceSegmentIndex<String> index = ScarceSegmentIndex
                .<String>builder()
                .withDirectory(
                        directory)
                .withFileName(FILE_NAME)//
                .withKeyTypeDescriptor(stringTd)//
                .build();

        index.openWriterTx().execute(writer -> {
            entries.forEach(writer::write);
        });

        return index;
    }
}
