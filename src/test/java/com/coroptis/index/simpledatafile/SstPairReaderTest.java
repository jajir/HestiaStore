package com.coroptis.index.simpledatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.CloseablePairReader;
import com.coroptis.index.datatype.TypeDescriptorInteger;

public class SstPairReaderTest {

    @SuppressWarnings("unchecked")
    private final SimpleDataFile<Integer, String> sdf = mock(
            SimpleDataFile.class);

    @SuppressWarnings("unchecked")
    private final CloseablePairReader<Integer, String> reader = mock(CloseablePairReader.class);

    @SuppressWarnings("unchecked")
    private final CloseablePairReader<Integer, String> reader2 = mock(CloseablePairReader.class);

    private final TypeDescriptorInteger intTd = new TypeDescriptorInteger();

    @Test
    public void test_simpleReading() throws Exception {
        when(sdf.openReader()).thenReturn(reader);
        final SstPairReader<Integer, String> spr = new SstPairReader<>(sdf,
                intTd.getComparator());
        assertFalse(spr.isDirty());
        
        when(reader.read()).thenReturn(Pair.of(1, "ahoj"));
        assertEquals(Pair.of(1, "ahoj"), spr.read());
        
        when(reader.read()).thenReturn(Pair.of(2, "lidi"));
        assertEquals(Pair.of(2, "lidi"), spr.read());
        
        when(reader.read()).thenReturn(null);
        assertNull(spr.read());
        
       spr.close();
       verify(reader).close();
       assertFalse(spr.isDirty());
       reset((Object)reader,reader2);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_setIsDirty() throws Exception {
        when(sdf.openReader()).thenReturn(reader);
        final SstPairReader<Integer, String> spr = new SstPairReader<>(sdf,
                intTd.getComparator());
        assertFalse(spr.isDirty());
        
        when(reader.read()).thenReturn(Pair.of(1, "ahoj"));
        assertEquals(Pair.of(1, "ahoj"), spr.read());
        
        when(reader.read()).thenReturn(Pair.of(2, "lidi"));
        assertEquals(Pair.of(2, "lidi"), spr.read());
        
        when(reader.read()).thenReturn(Pair.of(3, "planeta"));
        assertEquals(Pair.of(3, "planeta"), spr.read());
        
        spr.makeDirty();
        assertTrue(spr.isDirty());
        
        when(sdf.openReader()).thenReturn(reader2);
        when(reader2.read()).thenReturn(Pair.of(1, "letadlo"),Pair.of(2, "lidi"),Pair.of(3, "planeta"),Pair.of(4, "kolecko"));
        assertEquals(Pair.of(4, "kolecko"), spr.read());
        verify(reader).close();
        assertFalse(spr.isDirty());
        
        when(reader2.read()).thenReturn(null);
        assertNull(spr.read());
        assertNull(spr.read());
        
       spr.close();
       verify(reader2).close();
       assertFalse(spr.isDirty());
       reset((Object)reader,(Object)reader2);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_setIsDirty_new_file_have_less_items() throws Exception {
        when(sdf.openReader()).thenReturn(reader);
        final SstPairReader<Integer, String> spr = new SstPairReader<>(sdf,
                intTd.getComparator());
        assertFalse(spr.isDirty());
        
        when(reader.read()).thenReturn(Pair.of(1, "ahoj"));
        assertEquals(Pair.of(1, "ahoj"), spr.read());
        
        when(reader.read()).thenReturn(Pair.of(2, "lidi"));
        assertEquals(Pair.of(2, "lidi"), spr.read());
        
        when(reader.read()).thenReturn(Pair.of(3, "planeta"));
        assertEquals(Pair.of(3, "planeta"), spr.read());
        
        spr.makeDirty();
        assertTrue(spr.isDirty());
        
        when(sdf.openReader()).thenReturn(reader2);
        when(reader2.read()).thenReturn(Pair.of(1, "letadlo"),Pair.of(2, "lidi"),null);
        assertNull(spr.read());
        verify(reader).close();
        assertFalse(spr.isDirty());
        
        when(reader2.read()).thenReturn(null);
        assertNull(spr.read());
        assertNull(spr.read());
        
       spr.close();
       verify(reader2).close();
       assertFalse(spr.isDirty());
       reset((Object)reader,(Object)reader2);
    }

}
