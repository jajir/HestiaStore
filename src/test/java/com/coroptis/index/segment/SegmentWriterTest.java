package com.coroptis.index.segment;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.sstfile.SstFile;
import com.coroptis.index.sstfile.SstFileWriter;

@ExtendWith(MockitoExtension.class)
public class SegmentWriterTest {

    private final static String SEGMENT_CACHE_DELTA_FILE_1 = "segment-00027-delta-003.cache";
    private final static String SEGMENT_CACHE_DELTA_FILE_2 = "segment-00027-delta-004.cache";
    private final static Pair<Integer, String> PAIR_1 = Pair.of(1, "aaa");
    private final static Pair<Integer, String> PAIR_2 = Pair.of(2, "bbb");
    private final static Pair<Integer, String> PAIR_3 = Pair.of(3, "ccc");

    private final TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();

    @Mock
    private SegmentFiles<Integer, String> segmentFiles;

    @Mock
    private SegmentPropertiesManager segmentPropertiesManager;

    @Mock
    private SegmentCompacter<Integer, String> segmentCompacter;

    @Mock
    private SstFile<Integer, String> sstFile1;

    @Mock
    private SstFile<Integer, String> sstFile2;

    @Mock
    private SstFileWriter<Integer, String> sstFileWriter1;

    @Mock
    private SstFileWriter<Integer, String> sstFileWriter2;

    @Mock
    private SegmentSearcher<Integer, String> segmentSearcher;

    @Test
    public void test_basic_writing() throws Exception {
        when(segmentFiles.getKeyTypeDescriptor()).thenReturn(tdi);
        final SegmentWriter<Integer, String> segmentWriter = new SegmentWriter<>(
                segmentFiles, segmentPropertiesManager, 
                segmentCompacter);
        
        
        when(segmentPropertiesManager
                .getAndIncreaseDeltaFileName()).thenReturn(SEGMENT_CACHE_DELTA_FILE_1);
        when(segmentFiles.getCacheSstFile(SEGMENT_CACHE_DELTA_FILE_1)).thenReturn(sstFile1);
        when(sstFile1.openWriter()).thenReturn(sstFileWriter1);
        when(segmentCompacter.optionallyCompact()).thenReturn(false);
        when(segmentCompacter.shouldBeCompacted(1)).thenReturn(false);
        when(segmentCompacter.shouldBeCompacted(2)).thenReturn(false);
        when(segmentCompacter.shouldBeCompacted(3)).thenReturn(false);
        try(final PairWriter<Integer, String> writer= segmentWriter.openWriter()){
            writer.put(PAIR_1);
            writer.put(PAIR_2);
            writer.put(PAIR_3);
        }
        
        //verify that writing to cache delta file name was done 
        verify(sstFileWriter1).put(PAIR_1);
        verify(sstFileWriter1).put(PAIR_2);
        verify(sstFileWriter1).put(PAIR_3);
        
        //verify that segment properties are updated
        verify(segmentPropertiesManager).increaseNumberOfKeysInCache(3);
        verify(segmentPropertiesManager).flush();
        
        //Verify that segment compacter was correctly called
        verify(segmentCompacter, times(1)).optionallyCompact();
        verify(segmentCompacter).shouldBeCompacted(1);
        verify(segmentCompacter).shouldBeCompacted(2);
        verify(segmentCompacter).shouldBeCompacted(3);
        
    }

    @Test
    public void test_compact_during_writing() throws Exception {
        when(segmentFiles.getKeyTypeDescriptor()).thenReturn(tdi);
        final SegmentWriter<Integer, String> segmentWriter = new SegmentWriter<>(
                segmentFiles, segmentPropertiesManager,
                segmentCompacter);

        //first  delta file
        when(segmentPropertiesManager
                .getAndIncreaseDeltaFileName()).thenReturn(SEGMENT_CACHE_DELTA_FILE_1);
        when(segmentFiles.getCacheSstFile(SEGMENT_CACHE_DELTA_FILE_1)).thenReturn(sstFile1);
        when(sstFile1.openWriter()).thenReturn(sstFileWriter1);
        

        when(segmentCompacter.optionallyCompact()).thenReturn(false);
        when(segmentCompacter.shouldBeCompacted(1)).thenReturn(false);
        //when second pair is added segment cache is compacted
        when(segmentCompacter.shouldBeCompacted(2)).thenReturn(true);
        when(segmentCompacter.shouldBeCompacted(1)).thenReturn(false);
        try(final PairWriter<Integer, String> writer= segmentWriter.openWriter()){
            writer.put(PAIR_1);
            writer.put(PAIR_2);
            writer.put(PAIR_3);
            
            //second delta file
            when(segmentPropertiesManager
                    .getAndIncreaseDeltaFileName()).thenReturn(SEGMENT_CACHE_DELTA_FILE_2);
            when(segmentFiles.getCacheSstFile(SEGMENT_CACHE_DELTA_FILE_2)).thenReturn(sstFile2);
            when(sstFile2.openWriter()).thenReturn(sstFileWriter2);
        }
        
        //verify that writing to cache delta file name was done 
        verify(sstFileWriter1).put(PAIR_1);
        verify(sstFileWriter1).put(PAIR_2);
        verify(sstFileWriter2).put(PAIR_3);

        //verify that segment properties are updated 1
        verify(segmentPropertiesManager).increaseNumberOfKeysInCache(2);
        verify(segmentPropertiesManager,times(2)).flush();
        
        //verify that segment properties are updated 2
        verify(segmentPropertiesManager).increaseNumberOfKeysInCache(1);
        
        //Verify that segment compacter was correctly called
        verify(segmentCompacter, times(1)).optionallyCompact();
        verify(segmentCompacter, times(1)).forceCompact();;
        verify(segmentCompacter,times(2)).shouldBeCompacted(1);
        verify(segmentCompacter).shouldBeCompacted(2);
        
    }

    @BeforeEach
    void beforeEeachTest() {
    }

}
