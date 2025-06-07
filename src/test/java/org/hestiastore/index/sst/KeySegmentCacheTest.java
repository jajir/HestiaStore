package org.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.stream.Collectors;

import org.hestiastore.index.Pair;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KeySegmentCacheTest {

    private final TypeDescriptorString stringTd = new TypeDescriptorString();

    private Directory directory;

    @BeforeEach
    public void prepareData() {
        directory = new MemDirectory();
        try (KeySegmentCache<String> cache = new KeySegmentCache<>(directory,
                stringTd)) {
            cache.insertSegment("ahoj", SegmentId.of(1));
            cache.insertSegment("betka", SegmentId.of(2));
            cache.insertSegment("cukrar", SegmentId.of(3));
            cache.insertSegment("dikobraz", SegmentId.of(4));
            /*
             * Inserting of new higher key, should not add segment. In should
             * update key in higher segment key.
             */
            assertEquals(4, cache.insertKeyToSegment("kachna").getId());
        }
    }

    @AfterEach
    public void cleanData() {
        directory = null;
    }

    @Test
    public void test_constructor_empty_directory() throws Exception {
        assertThrows(NullPointerException.class, () -> {
            try (KeySegmentCache<String> fif = new KeySegmentCache<>(null,
                    stringTd)) {
            }
        });
    }

    @Test
    public void test_constructor_empty_keyTypeDescriptor() throws Exception {
        assertThrows(NullPointerException.class, () -> {
            try (KeySegmentCache<String> fif = new KeySegmentCache<>(directory,
                    null)) {
            }
        });
    }

    @Test
    public void test_insertSegment_duplicate_segmentId() throws Exception {
        try (KeySegmentCache<String> fif = new KeySegmentCache<>(directory,
                stringTd)) {
            assertThrows(IllegalArgumentException.class,
                    () -> fif.insertSegment("aaa", SegmentId.of(1)),
                    "Segment id 'segment-00001' already exists");
        }
    }

    @Test
    public void test_insertKeyToSegment_higher_segment() throws Exception {
        try (KeySegmentCache<String> fif = new KeySegmentCache<>(directory,
                stringTd)) {
            assertEquals(4, fif.insertKeyToSegment("zzz").getId());
            assertEquals(4, fif.findSegmentId("zzz").getId());
            assertEquals(4, fif.findSegmentId("zzz").getId());
            /*
             * Verify that higher page key was updated.
             */
            final List<Pair<String, SegmentId>> list = fif.getSegmentsAsStream()
                    .collect(Collectors.toList());
            assertEquals(Pair.of("zzz", SegmentId.of(4)), list.get(3));
        }
    }

    @Test
    public void test_insetSegment_normal() throws Exception {
        try (KeySegmentCache<String> fif = new KeySegmentCache<>(directory,
                stringTd)) {
            assertEquals(4, fif.insertKeyToSegment("zzz").getId());
            assertEquals(4, fif.findSegmentId("zzz").getId());
            assertEquals(4, fif.findSegmentId("zzz").getId());
            /*
             * Verify that higher page key was updated.
             */
            final List<Pair<String, SegmentId>> list = fif.getSegmentsAsStream()
                    .collect(Collectors.toList());
            assertEquals(Pair.of("zzz", SegmentId.of(4)), list.get(3));
        }
    }

    @Test
    public void test_getSegmentsAsStream_print_data() throws Exception {
        try (KeySegmentCache<String> fif = new KeySegmentCache<>(directory,
                stringTd)) {
            final List<Pair<String, SegmentId>> segments = fif
                    .getSegmentsAsStream().toList();
            assertEquals("Pair[key='ahoj',value='segment-00001']",
                    segments.get(0).toString());
            assertEquals("Pair[key='betka',value='segment-00002']",
                    segments.get(1).toString());
            assertEquals("Pair[key='cukrar',value='segment-00003']",
                    segments.get(2).toString());
            assertEquals("Pair[key='kachna',value='segment-00004']",
                    segments.get(3).toString());
        }
    }

    @Test
    public void test_getSegmentsAsStream_number_of_segments() throws Exception {
        try (KeySegmentCache<String> fif = new KeySegmentCache<>(directory,
                stringTd)) {
            assertEquals(4, fif.getSegmentsAsStream().count());
        }
    }

    @Test
    public void test_getSegmentsAsStream_correct_page_order() throws Exception {
        try (KeySegmentCache<String> fif = new KeySegmentCache<>(directory,
                stringTd)) {
            /*
             * Verify that pages are returned as sorted stream.
             */
            final List<Pair<String, SegmentId>> list = fif.getSegmentsAsStream()
                    .collect(Collectors.toList());
            assertEquals(Pair.of("ahoj", SegmentId.of(1)), list.get(0));
            assertEquals(Pair.of("betka", SegmentId.of(2)), list.get(1));
            assertEquals(Pair.of("cukrar", SegmentId.of(3)), list.get(2));
            assertEquals(Pair.of("kachna", SegmentId.of(4)), list.get(3));
        }
    }

    @Test
    public void test_findSegmentId() throws Exception {
        try (KeySegmentCache<String> fif = new KeySegmentCache<>(directory,
                stringTd)) {
            assertEquals(3, fif.findSegmentId("cuketa").getId());
            assertEquals(3, fif.findSegmentId("bziknout").getId());
            assertEquals(4, fif.findSegmentId("kachna").getId());
            assertEquals(2, fif.findSegmentId("backora").getId());
            assertEquals(1, fif.findSegmentId("ahoj").getId());
            assertEquals(1, fif.findSegmentId("aaaaa").getId());
            assertEquals(1, fif.findSegmentId("a").getId());

            assertNull(fif.findSegmentId("zzz"));
        }
    }

    @Test
    public void test_getSegmentIds_noWindow() throws Exception {
        try (KeySegmentCache<String> fif = new KeySegmentCache<>(directory,
                stringTd)) {
            final List<SegmentId> list = fif.getSegmentIds();
            assertEquals(1, list.get(0).getId());
            assertEquals(2, list.get(1).getId());
            assertEquals(3, list.get(2).getId());
            assertEquals(4, list.get(3).getId());
            assertEquals(4, list.size());
        }
    }

    @Test
    public void test_getSegmentIds_offSet_1() throws Exception {
        try (KeySegmentCache<String> fif = new KeySegmentCache<>(directory,
                stringTd)) {
            final List<SegmentId> list = fif
                    .getSegmentIds(SegmentWindow.ofOffset(1));
            assertEquals(2, list.get(0).getId());
            assertEquals(3, list.get(1).getId());
            assertEquals(4, list.get(2).getId());
            assertEquals(3, list.size());
        }
    }

    @Test
    public void test_getSegmentIds_offSet_1_limit_2() throws Exception {
        try (KeySegmentCache<String> fif = new KeySegmentCache<>(directory,
                stringTd)) {
            final List<SegmentId> list = fif
                    .getSegmentIds(SegmentWindow.of(1, 2));
            assertEquals(2, list.get(0).getId());
            assertEquals(3, list.get(1).getId());
            assertEquals(2, list.size());
        }
    }

    @Test
    public void test_getSegmentIds_offSet_78_limit_2() throws Exception {
        try (KeySegmentCache<String> fif = new KeySegmentCache<>(directory,
                stringTd)) {
            final List<SegmentId> list = fif
                    .getSegmentIds(SegmentWindow.of(78, 2));
            assertEquals(0, list.size());
        }
    }

}
