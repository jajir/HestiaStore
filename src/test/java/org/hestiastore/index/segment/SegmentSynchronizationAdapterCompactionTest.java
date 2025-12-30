package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSynchronizationAdapterCompactionTest {

    @Mock
    private SegmentFiles<Integer, String> segmentFiles;
    @Mock
    private SegmentConf segmentConf;
    @Mock
    private VersionController versionController;
    @Mock
    private SegmentPropertiesManager segmentPropertiesManager;
    @Mock
    private SegmentResources<Integer, String> segmentResources;
    @Mock
    private SegmentDeltaCacheController<Integer, String> deltaCacheController;
    @Mock
    private SegmentSearcher<Integer, String> segmentSearcher;
    @Mock
    private SegmentCompactionPolicyWithManager compactionPolicy;
    @Mock
    private SegmentCompacter<Integer, String> segmentCompacter;
    @Mock
    private SegmentReplacer<Integer, String> segmentReplacer;
    @Mock
    private SegmentSplitterPolicy<Integer, String> segmentSplitterPolicy;

    @Test
    void request_compaction_runs_async_under_adapter() throws Exception {
        when(segmentFiles.getKeyTypeDescriptor())
                .thenReturn(new TypeDescriptorInteger());
        when(segmentFiles.getValueTypeDescriptor())
                .thenReturn(new TypeDescriptorShortString());
        final SegmentDeltaCache<Integer, String> deltaCache = org.mockito.Mockito
                .mock(SegmentDeltaCache.class);
        when(deltaCache.getAsSortedList()).thenReturn(java.util.List.<Entry<Integer, String>>of());
        when(segmentResources.getSegmentDeltaCache()).thenReturn(deltaCache);
        final SegmentImpl<Integer, String> segment = new SegmentImpl<>(
                segmentFiles, segmentConf, versionController,
                segmentPropertiesManager, segmentResources,
                deltaCacheController, segmentSearcher, compactionPolicy,
                segmentCompacter, segmentReplacer, segmentSplitterPolicy);
        final SegmentImpl<Integer, String> spySegment = spy(segment);
        final CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(spySegment).forceCompact();

        try (SegmentSynchronizationAdapter<Integer, String> adapter = new SegmentSynchronizationAdapter<>(
                spySegment)) {
            spySegment.requestCompaction();
            assertTrue(latch.await(2, TimeUnit.SECONDS),
                    "Compaction did not run asynchronously");
        }
    }
}
