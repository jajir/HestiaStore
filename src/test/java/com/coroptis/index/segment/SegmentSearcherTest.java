package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.OptimisticLockObjectVersionProvider;
import com.coroptis.index.Pair;
import com.coroptis.index.PairReader;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.sstfile.SstFile;

@ExtendWith(MockitoExtension.class)
public class SegmentSearcherTest {

    private final TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();

    private final TypeDescriptor<String> tds = new TypeDescriptorString();

    @Mock
    private SegmentFiles<Integer, String> segmentFiles;

    @Mock
    private SegmentConf segmentConf;

    @Mock
    private SegmentPropertiesManager segmentPropertiesManager;

    @Mock
    private SstFile<Integer, String> sstFile;

    @Mock
    private SegmentIndexSearcherSupplier<Integer, String> segmentIndexSearcherSupplier;

    @Mock
    private PairReader<Integer, String> pairReader;

    @Mock
    private Directory directory;

    @Test
    public void test_simple_get_by_key() throws Exception {
        final TestVersionProvider versionProvider = new TestVersionProvider();
        prepareOpeningSearcher();
        try (final SegmentSearcher<Integer, String> searcher = new SegmentSearcher<>(
                segmentFiles, segmentConf, versionProvider,
                segmentPropertiesManager, segmentIndexSearcherSupplier)) {

            assertEquals("hello", searcher.get(37));
            assertEquals(null, searcher.get(5));
            assertEquals("hello", searcher.get(37));
        }

        verify(segmentFiles, times(1)).getCacheSstFile();
        verify(segmentFiles, times(1)).getScarceFileName();
        verify(segmentFiles, times(1)).getBloomFilterFileName();
    }

    @Test
    public void test_optimistic_lock_invalidating() throws Exception {
        final TestVersionProvider versionProvider = new TestVersionProvider();
        prepareOpeningSearcher();

        try (final SegmentSearcher<Integer, String> searcher = new SegmentSearcher<>(
                segmentFiles, segmentConf, versionProvider,
                segmentPropertiesManager, segmentIndexSearcherSupplier)) {

            assertEquals("hello", searcher.get(37));
            when(pairReader.read()).thenReturn(Pair.of(17, "duck"))
                    .thenReturn(null);
            versionProvider.changeVerson();
            assertEquals("duck", searcher.get(17));
            assertEquals("duck", searcher.get(17));
        }

        verify(segmentFiles, times(2)).getCacheSstFile();
        verify(segmentFiles, times(2)).getScarceFileName();
        verify(segmentFiles, times(2)).getBloomFilterFileName();
    }

    private void prepareOpeningSearcher() {
        when(segmentFiles.getId()).thenReturn(SegmentId.of(7));
        when(segmentFiles.getKeyTypeDescriptor()).thenReturn(tdi);
        when(segmentFiles.getValueTypeDescriptor()).thenReturn(tds);
        when(segmentFiles.getCacheSstFile()).thenReturn(sstFile);
        when(sstFile.openReader()).thenReturn(pairReader);
        when(pairReader.read()).thenReturn(Pair.of(37, "hello"))
                .thenReturn(null);
        when(segmentFiles.getDirectory()).thenReturn(directory);
        when(segmentFiles.getScarceFileName())
                .thenReturn("segment-00007.scarce");
        when(segmentFiles.getBloomFilterFileName())
                .thenReturn("segment-00007.bloom-filter");        
    }

    static class TestVersionProvider
            implements OptimisticLockObjectVersionProvider {

        private int version;

        void changeVerson() {
            version++;
        }

        @Override
        public int getVersion() {
            return version;
        }

    }

}
