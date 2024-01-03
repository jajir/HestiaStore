package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.segment.SegmentId;

public class KeySegmentCacheTest {

    private final Logger logger = LoggerFactory
            .getLogger(KeySegmentCacheTest.class);

    private final TypeDescriptorString stringTd = new TypeDescriptorString();

    private Directory directory;

    @BeforeEach
    public void prepareData() {
        directory = new MemDirectory();
        try (final KeySegmentCache<String> cache = new KeySegmentCache<>(
                directory, stringTd)) {
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
            try (final KeySegmentCache<String> fif = new KeySegmentCache<>(null,
                    stringTd)) {
            }
        });
    }

    @Test
    public void test_constructor_empty_keyTypeDescriptor() throws Exception {
        assertThrows(NullPointerException.class, () -> {
            try (final KeySegmentCache<String> fif = new KeySegmentCache<>(
                    directory, null)) {
            }
        });
    }

    @Test
    public void test_insertSegment_duplicate_segmentId() throws Exception {
        try (final KeySegmentCache<String> fif = new KeySegmentCache<>(
                directory, stringTd)) {
            assertThrows(IllegalArgumentException.class,
                    () -> fif.insertSegment("aaa", SegmentId.of(1)),
                    "Segment id 'segment-00001' already exists");
        }
    }

    @Test
    public void test_insertKeyToSegment_higher_segment() throws Exception {
        try (final KeySegmentCache<String> fif = new KeySegmentCache<>(
                directory, stringTd)) {
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
        try (final KeySegmentCache<String> fif = new KeySegmentCache<>(
                directory, stringTd)) {
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
        try (final KeySegmentCache<String> fif = new KeySegmentCache<>(
                directory, stringTd)) {
            fif.getSegmentsAsStream().forEach(p -> {
                logger.debug("Segment '{}'", p.toString());
            });
        }
    }

    @Test
    public void test_getSegmentsAsStream_number_of_segments() throws Exception {
        try (final KeySegmentCache<String> fif = new KeySegmentCache<>(
                directory, stringTd)) {
            assertEquals(4, fif.getSegmentsAsStream().count());
        }
    }

    @Test
    public void test_getSegmentsAsStream_correct_page_order() throws Exception {
        try (final KeySegmentCache<String> fif = new KeySegmentCache<>(
                directory, stringTd)) {
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
        try (final KeySegmentCache<String> fif = new KeySegmentCache<>(
                directory, stringTd)) {
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

}
