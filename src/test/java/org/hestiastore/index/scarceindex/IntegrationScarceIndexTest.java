package org.hestiastore.index.scarceindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;

import org.hestiastore.index.Pair;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class IntegrationScarceIndexTest {

    private static final String FILE_NAME = "pok.dat";
    private final TypeDescriptorShortString stringTd = new TypeDescriptorShortString();

    private static final Pair<String, Integer> P_BBB_1 = Pair.of("bbb", 13);
    private static final Pair<String, Integer> P_BBB_2 = Pair.of("bbb", 1);
    private static final Pair<String, Integer> P_CCC_1 = Pair.of("ccc", 2);
    private static final Pair<String, Integer> P_CCC_2 = Pair.of("ccc", 3);
    private static final Pair<String, Integer> P_DDD = Pair.of("ddd", 3);
    private static final Pair<String, Integer> P_EEE = Pair.of("eee", 4);
    private static final Pair<String, Integer> P_FFF_1 = Pair.of("fff", 4);
    private static final Pair<String, Integer> P_FFF_2 = Pair.of("fff", 5);

    @Test
    void test_one_key() {
        final ScarceIndex<String> index = makeIndex(List.of(P_BBB_1));

        assertEquals(13, index.get("bbb"));
        assertNull(index.get("ddd"));
        assertNull(index.get("ccc"));

        assertEquals("bbb", index.getMinKey());
        assertEquals("bbb", index.getMaxKey());
    }

    @Test
    void test_empty() {
        final ScarceIndex<String> index = makeIndex(Collections.emptyList());

        assertNull(index.get("aaa"));
        assertNull(index.get("bbb"));
        assertNull(index.get("ccc"));

        assertNull(index.getMinKey());
        assertNull(index.getMaxKey());
    }

    @Test
    void test_one_multiple() {
        final ScarceIndex<String> index = makeIndex(
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
        final List<Pair<String, Integer>> pairs = List.of(P_BBB_2, P_CCC_1,
                P_CCC_2, P_EEE, P_FFF_2);
        assertThrows(IllegalArgumentException.class, () -> makeIndex(pairs));
    }

    @Test
    void test_sanity_check() {
        final List<Pair<String, Integer>> pairs = List.of(P_BBB_2, P_CCC_1,
                P_DDD, P_EEE, P_FFF_1);
        assertThrows(IllegalStateException.class, () -> makeIndex(pairs));
    }

    @Test
    void test_overwrite_index() {

        final ScarceIndex<String> index = makeIndex(
                List.of(P_BBB_2, P_CCC_1, P_DDD, P_EEE, P_FFF_2));

        try (ScarceIndexWriter<String> writer = index.openWriter()) {
            writer.put(P_BBB_2);
        }

        assertEquals(1, index.get("aaa"));
        assertEquals(1, index.get("bbb"));
        assertNull(index.get("ccc"));
    }

    private ScarceIndex<String> makeIndex(
            final List<Pair<String, Integer>> pairs) {
        final MemDirectory directory = new MemDirectory();
        final ScarceIndex<String> index = ScarceIndex.<String>builder()
                .withDirectory(directory).withFileName(FILE_NAME)//
                .withKeyTypeDescriptor(stringTd)//
                .build();

        try (ScarceIndexWriter<String> writer = index.openWriter()) {
            pairs.forEach(writer::put);
        }

        return index;
    }
}
