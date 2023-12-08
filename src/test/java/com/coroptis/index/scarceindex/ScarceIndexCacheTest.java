package com.coroptis.index.scarceindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeDescriptorString;

public class ScarceIndexCacheTest {

private final TypeDescriptorString tds = new TypeDescriptorString();

        @Test
    public void test_constructor_null() throws Exception {
    assertThrows(NullPointerException.class,
    () -> new ScarceIndexCache<>(null));
    }

        @Test
    public void test_empty_index() throws Exception {
    final ScarceIndexCache<String> cache = makeCache(Collections.emptyList());
        assertNull(cache.findSegmentId("a"));
        assertNull(cache.findSegmentId("aaaaaaaaaaaaaaaaa"));
        assertNull(cache.findSegmentId("zzz"));

        cache.sanityCheck();
        assertEquals(0, cache.getKeyCount());
       assertNull(cache.getMaxKey());
       assertNull(cache.getMinKey());
        assertEquals(0, cache.getSegmentsAsStream().count());
    }


        @Test
    public void test_simple_index() throws Exception {
    final ScarceIndexCache<String> cache = makeCache(List.of(Pair.of("bbb", 1), Pair.of("ccc", 2), Pair.of("ddd", 3),
    Pair.of("eee", 4), Pair.of("fff", 5)));
        assertEquals(1, cache.findSegmentId("bbb"));
        assertEquals(1, cache.findSegmentId("bbbb"));
        assertEquals(2, cache.findSegmentId("ccc"));
        assertEquals(2, cache.findSegmentId("cccb"));
        assertEquals(5, cache.findSegmentId("fff"));
        assertNull(cache.findSegmentId("zzz"));

        cache.sanityCheck();
        assertEquals(5, cache.getKeyCount());
        assertEquals("bbb", cache.getMinKey());
        assertEquals("fff", cache.getMaxKey());
        assertEquals(5, cache.getSegmentsAsStream().count());
    }



        private ScarceIndexCache<String> makeCache(
            final List<Pair<String, Integer>> pairs) {
    ScarceIndexCache<String> cache = new ScarceIndexCache<>(tds);

            pairs.forEach(cache::put);
      
        return cache;
    }

}
