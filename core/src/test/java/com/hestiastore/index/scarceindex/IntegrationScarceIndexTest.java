package com.hestiastore.index.scarceindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.hestiastore.index.Pair;
import com.hestiastore.index.datatype.TypeDescriptorString;
import com.hestiastore.index.directory.MemDirectory;

public class IntegrationScarceIndexTest {

    private final static String FILE_NAME = "pok.dat";
    private final TypeDescriptorString stringTd = new TypeDescriptorString();

    @Test
    public void test_one_key() throws Exception {
        final ScarceIndex<String> index = makeIndex(
                List.of(Pair.of("bbb", 13)));

        assertEquals(13, index.get("bbb"));
        assertNull(index.get("aaa"));
        assertNull(index.get("ccc"));

        assertEquals("bbb", index.getMinKey());
        assertEquals("bbb", index.getMaxKey());
    }

    @Test
    public void test_empty() throws Exception {
        final ScarceIndex<String> index = makeIndex(Collections.emptyList());

        assertNull(index.get("aaa"));
        assertNull(index.get("bbb"));
        assertNull(index.get("ccc"));

        assertNull(index.getMinKey());
        assertNull(index.getMaxKey());
    }

    @Test
    public void test_one_multiple() throws Exception {
        final ScarceIndex<String> index = makeIndex(
                List.of(Pair.of("bbb", 1), Pair.of("ccc", 2), Pair.of("ddd", 3),
                        Pair.of("eee", 4), Pair.of("fff", 5)));

        assertNull(index.get("aaa"));
        assertEquals(1, index.get("bbb"));
        assertEquals(2, index.get("ccc"));
        assertEquals(2, index.get("ccd"));
        assertEquals(2, index.get("cee"));
        assertEquals(5, index.get("fff"));
        assertNull(index.get("ggg"));
        assertEquals("bbb", index.getMinKey());
        assertEquals("fff", index.getMaxKey());
    }

    @Test
    public void test_insert_duplicite_keys() throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> makeIndex(List.of(Pair.of("bbb", 1), Pair.of("ccc", 2),
                        Pair.of("ccc", 3), Pair.of("eee", 4),
                        Pair.of("fff", 5))));
    }

    @Test
    public void test_sanity_check() throws Exception {
        assertThrows(IllegalStateException.class,
                () -> makeIndex(List.of(Pair.of("bbb", 1), Pair.of("ccc", 2),
                        Pair.of("ddd", 3), Pair.of("eee", 4),
                        Pair.of("fff", 4))));
    }

    @Test
    public void test_overwrite_index() throws Exception {

        final ScarceIndex<String> index = makeIndex(
                List.of(Pair.of("bbb", 1), Pair.of("ccc", 2), Pair.of("ddd", 3),
                        Pair.of("eee", 4), Pair.of("fff", 5)));

        try (ScarceIndexWriter<String> writer = index.openWriter()) {
            writer.put(Pair.of("bbb", 1));
        }

        assertEquals(1, index.get("bbb"));
        assertNull(index.get("aaa"));
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
