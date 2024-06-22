package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SegmentDeltaCachePairIteratorTest {

    private final List<Integer> testKeys = Arrays.asList(1, 2, 3);

    @Mock
    private SegmentDeltaCacheController<Integer, String> deltaCacheController;

    @Mock
    private SegmentDeltaCache<Integer, String> deltaCache;

    @Test
    void test_simple() throws Exception {
        when(deltaCacheController.getDeltaCache()).thenReturn(deltaCache);
        when(deltaCache.get(1)).thenReturn("aaa");
        when(deltaCache.get(2)).thenReturn("bbb");
        when(deltaCache.get(3)).thenReturn("ccc");
        try(final SegmentDeltaCachePairIterator<Integer, String> iterator = new SegmentDeltaCachePairIterator<>(
                testKeys, deltaCacheController)){
        
        assertTrue(iterator.hasNext());
        assertEquals("aaa", iterator.next().getValue());
        
        assertTrue(iterator.hasNext());
        assertEquals("bbb", iterator.next().getValue());
        
        assertTrue(iterator.hasNext());
          assertEquals("ccc", iterator.next().getValue());
        
        assertFalse(iterator.hasNext());}
    }

}
