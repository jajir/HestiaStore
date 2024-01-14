package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    private final static Pair<Integer, String> PAIR_1 = Pair.of(1, "aaa");
    private final static Pair<Integer, String> PAIR_2 = Pair.of(2, "bbb");
    private final static Pair<Integer, String> PAIR_3 = Pair.of(3, "ccc");

    private final TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();

    private VersionController versionController;

    @Mock
    private SegmentFiles<Integer, String> segmentFiles;

    @Mock
    private SegmentPropertiesManager segmentPropertiesManager;

    @Mock
    private SegmentCompacter<Integer, String> segmentCompacter;

    @Mock
    private SstFile<Integer, String> sstFile;

    @Mock
    private SstFileWriter<Integer, String> sstFileWriter;

    @Test
    public void test_basic_writing() throws Exception {
        when(segmentFiles.getKeyTypeDescriptor()).thenReturn(tdi);
        final SegmentWriter<Integer, String> segmentWriter = new SegmentWriter<>(
                segmentFiles, segmentPropertiesManager, versionController,
                segmentCompacter);
        
        
        when(segmentPropertiesManager
                .getAndIncreaseDeltaFileName()).thenReturn(SEGMENT_CACHE_DELTA_FILE_1);
        when(segmentFiles.getCacheSstFile(SEGMENT_CACHE_DELTA_FILE_1)).thenReturn(sstFile);
        when(sstFile.openWriter()).thenReturn(sstFileWriter);
        when(segmentCompacter.optionallyCompact()).thenReturn(false);
        when(segmentCompacter.optionallyCompact(0)).thenReturn(false);
        when(segmentCompacter.optionallyCompact(1)).thenReturn(false);
        when(segmentCompacter.optionallyCompact(2)).thenReturn(false);
        try(final PairWriter<Integer, String> writer= segmentWriter.openWriter()){
            writer.put(PAIR_1);
            writer.put(PAIR_2);
            writer.put(PAIR_3);
        }
        
        //verify that writing to cache delta file name was done 
        verify(sstFileWriter).put(PAIR_1);
        verify(sstFileWriter).put(PAIR_2);
        verify(sstFileWriter).put(PAIR_3);
        
        //verify that segment properties are updated
        verify(segmentPropertiesManager).increaseNumberOfKeysInCache(3);
        verify(segmentPropertiesManager).flush();
        
        //Verify that segment compacter was correctly called
        verify(segmentCompacter, times(1)).optionallyCompact();
        verify(segmentCompacter).optionallyCompact(0);
        verify(segmentCompacter).optionallyCompact(1);
        verify(segmentCompacter).optionallyCompact(2);
        
        //Verify that segment version was increased 
        assertEquals(1,versionController.getVersion());
    }

    @BeforeEach
    void beforeEeachTest() {
        versionController = new VersionController();
    }

}
