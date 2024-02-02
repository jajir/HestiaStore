package com.coroptis.index.simpledatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Comparator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.Pair;
import com.coroptis.index.PairReader;

@ExtendWith(MockitoExtension.class)
public class MergedPairReaderTest {

    @Mock
    private PairReader<Integer, String> reader1;

    @Mock
    private PairReader<Integer, String> reader2;

    // FIXME there is used same mock instance in all tests

    @Test
    public void test_reader2_is_empty() throws Exception {
        when(reader1.read()).thenReturn(Pair.of(1, "a"));
        when(reader2.read()).thenReturn(null);
        final MergedPairReader<Integer, String> reader = new MergedPairReader<>(reader1, reader2,
                (k, s1, s2) -> s2, Comparator.naturalOrder());
        
        when(reader1.read()).thenReturn(Pair.of(2, "c"));
        assertEquals(Pair.of(1, "a"), reader.read());
        when(reader1.read()).thenReturn(Pair.of(3, "d"));
        assertEquals(Pair.of(2, "c"), reader.read());
        when(reader1.read()).thenReturn(null);
        assertEquals(Pair.of(3, "d"), reader.read());
        assertNull(reader.read());
     
        reader.close();
    }

    @Test
    public void test_reader1_is_empty() throws Exception {
        when(reader1.read()).thenReturn(null);
        when(reader2.read()).thenReturn(Pair.of(1, "a"));
        final MergedPairReader<Integer, String> reader = new MergedPairReader<>(reader1, reader2,
                (k, s1, s2) -> s2, Comparator.naturalOrder());
        
        when(reader2.read()).thenReturn(Pair.of(2, "c"));
        assertEquals(Pair.of(1, "a"), reader.read());
        when(reader2.read()).thenReturn(Pair.of(3, "d"));
        assertEquals(Pair.of(2, "c"), reader.read());
        when(reader2.read()).thenReturn(null);
        assertEquals(Pair.of(3, "d"), reader.read());
        assertNull(reader.read());
     
        reader.close();
    }

    @Test
    public void test_reader2_have_bigger_key() throws Exception {
        when(reader1.read())
            .thenReturn(Pair.of(1, "a"))
            .thenReturn(Pair.of(2, "b"))
            .thenReturn(Pair.of(3, "c"))
            .thenReturn(null)
            ;
        when(reader2.read())
            .thenReturn(Pair.of(5, "d"))
            .thenReturn(null)
            ;
        final MergedPairReader<Integer, String> reader = new MergedPairReader<>(reader1, reader2,
                (k, s1, s2) -> s2, Comparator.naturalOrder());
        
        assertEquals(Pair.of(1, "a"), reader.read());
        assertEquals(Pair.of(2, "b"), reader.read());
        assertEquals(Pair.of(3, "c"), reader.read());
        assertEquals(Pair.of(5, "d"), reader.read());
        assertNull(reader.read());
     
        reader.close();
    }

    @Test
    public void test_order() throws Exception {
        when(reader1.read())
            .thenReturn(Pair.of(1, "a"))
            .thenReturn(Pair.of(2, "c"))
            .thenReturn(null)
            ;
        when(reader2.read())
            .thenReturn(Pair.of(2, "b"))
            .thenReturn(Pair.of(3, "d"))
            .thenReturn(null)
            ;
        final MergedPairReader<Integer, String> reader = new MergedPairReader<>(reader1, reader2,
                (k, s1, s2) -> s2, Comparator.naturalOrder());
        
        assertEquals(Pair.of(1, "a"), reader.read());
        assertEquals(Pair.of(2, "b"), reader.read());
        assertEquals(Pair.of(3, "d"), reader.read());
        assertNull(reader.read());
     
        reader.close();
    }

}
