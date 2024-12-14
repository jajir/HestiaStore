package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.Pair;
import com.coroptis.index.PairReader;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorString;

@ExtendWith(MockitoExtension.class)
public class PairReaderRefreshedFromCacheTest {

    private final static Pair<Integer, String> pair2 = Pair.of(2,"bbb");
    private final static Pair<Integer, String> pair3 = Pair.of(3,"ccc");
    private final static Pair<Integer, String> pair4 = Pair.of(4,"ddd");
    
    private final static TypeDescriptor<String> std = new TypeDescriptorString();

    @Mock
    private PairReader<Integer, String> segmentReader;

    @Mock
    private UniqueCache<Integer, String> cache;

    @Test
    void test_get_from_segment_and_not_in_cache() {
        final PairReaderRefreshedFromCache<Integer, String> supplier = new PairReaderRefreshedFromCache<>(
                segmentReader,
                cache, std);
        
        when(segmentReader.read()).thenReturn(pair2);
        when(cache.get(2)).thenReturn(null);

        assertEquals(pair2, supplier.read());
    }

    @Test
    void test_get_from_segment_and_updated_in_cache() {
        final PairReaderRefreshedFromCache<Integer, String> supplier = new PairReaderRefreshedFromCache<>(
                segmentReader,
                cache,std);
        
        when(segmentReader.read()).thenReturn(pair2);
        when(cache.get(2)).thenReturn("eee");

        assertEquals(Pair.of(2, "eee"), supplier.read());
    }

    @Test
    void test_get_not_in_segment() {
        final PairReaderRefreshedFromCache<Integer, String> supplier = new PairReaderRefreshedFromCache<>(
                segmentReader,
                cache, std);
        
        when(segmentReader.read()).thenReturn(null);

        assertNull(supplier.read());
    }

    
    @Test
    void test_get_from_segment_and_deleted_in_cache_not_other_pair_in_segment() {
        final PairReaderRefreshedFromCache<Integer, String> supplier = new PairReaderRefreshedFromCache<>(
                segmentReader,
                cache, std);
        
        when(segmentReader.read()).thenReturn(pair2).thenReturn(null);
        when(cache.get(2)).thenReturn(std.getTombstone());

        assertNull(supplier.read());
    }
    
    @Test
    void test_two_pairs_are_deleted_third_is_ok() {
        final PairReaderRefreshedFromCache<Integer, String> supplier = new PairReaderRefreshedFromCache<>(
                segmentReader,
                cache, std);
        
        when(segmentReader.read()).thenReturn(pair2).thenReturn(pair3).thenReturn(pair4);
        when(cache.get(2)).thenReturn(std.getTombstone());
        when(cache.get(3)).thenReturn(std.getTombstone());
        when(cache.get(4)).thenReturn(null);
        
        assertEquals(pair4, supplier.read());
    }
    @Test
    void test_three_pairs_are_deleted() {
        final PairReaderRefreshedFromCache<Integer, String> supplier = new PairReaderRefreshedFromCache<>(
                segmentReader,
                cache, std);
        
        when(segmentReader.read()).thenReturn(pair2).thenReturn(pair3).thenReturn(pair4).thenReturn(null);
        when(cache.get(2)).thenReturn(std.getTombstone());
        when(cache.get(3)).thenReturn(std.getTombstone());
        when(cache.get(4)).thenReturn(std.getTombstone());

        assertNull(supplier.read());
    }

}
