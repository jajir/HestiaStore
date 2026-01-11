package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexCoreTest {

    @Mock
    private SegmentRegistry<String, String> segmentRegistry;

    @Mock
    private SegmentMaintenanceCoordinator<String, String> maintenanceCoordinator;

    @Mock
    private Segment<String, String> segment;

    private AsyncDirectory asyncDirectory;
    private KeySegmentCache<String> keySegmentCache;
    private SegmentIndexCore<String, String> core;

    @BeforeEach
    void setUp() {
        asyncDirectory = AsyncDirectoryAdapter.wrap(new MemDirectory());
        keySegmentCache = new KeySegmentCache<>(asyncDirectory,
                new TypeDescriptorShortString());
        core = new SegmentIndexCore<>(keySegmentCache, segmentRegistry,
                maintenanceCoordinator);
    }

    @AfterEach
    void tearDown() {
        if (keySegmentCache != null && !keySegmentCache.wasClosed()) {
            keySegmentCache.close();
        }
        if (asyncDirectory != null && !asyncDirectory.wasClosed()) {
            asyncDirectory.close();
        }
        core = null;
        keySegmentCache = null;
        asyncDirectory = null;
    }

    @Test
    void get_returnsOkNullWhenNoSegmentMapping() {
        final IndexResult<String> result = core.get("missing");

        assertEquals(IndexResultStatus.OK, result.getStatus());
        assertNull(result.getValue());
        verifyNoInteractions(segmentRegistry);
    }

    @Test
    void get_returnsBusyWhenSegmentIsBusy() {
        final SegmentId segmentId = keySegmentCache.insertKeyToSegment("key");
        when(segmentRegistry.getSegment(segmentId)).thenReturn(segment);
        when(segment.get("key")).thenReturn(SegmentResult.busy());

        final IndexResult<String> result = core.get("key");

        assertEquals(IndexResultStatus.BUSY, result.getStatus());
    }

    @Test
    void put_schedulesMaintenanceOnSuccess() {
        final SegmentId segmentId = keySegmentCache.insertKeyToSegment("key");
        final KeySegmentCache.Snapshot<String> snapshot = keySegmentCache
                .snapshot();
        when(segmentRegistry.getSegment(segmentId)).thenReturn(segment);
        when(segment.put("key", "value")).thenReturn(SegmentResult.ok());

        final IndexResult<Void> result = core.put("key", "value");

        assertEquals(IndexResultStatus.OK, result.getStatus());
        verify(maintenanceCoordinator).handlePostWrite(segment, "key",
                segmentId, snapshot.version());
    }

    @Test
    void put_returnsBusyWhenSegmentIsBusy() {
        final SegmentId segmentId = keySegmentCache.insertKeyToSegment("key");
        when(segmentRegistry.getSegment(segmentId)).thenReturn(segment);
        when(segment.put("key", "value")).thenReturn(SegmentResult.busy());

        final IndexResult<Void> result = core.put("key", "value");

        assertEquals(IndexResultStatus.BUSY, result.getStatus());
        verifyNoInteractions(maintenanceCoordinator);
    }

    @Test
    void openIterator_returnsValueWhenOk() {
        final SegmentId segmentId = keySegmentCache.insertKeyToSegment("key");
        final EntryIterator<String, String> iterator = EntryIterator
                .make(List.<Entry<String, String>>of().iterator());
        when(segmentRegistry.getSegment(segmentId)).thenReturn(segment);
        when(segment.openIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(SegmentResult.ok(iterator));

        final IndexResult<EntryIterator<String, String>> result = core
                .openIterator(segmentId, SegmentIteratorIsolation.FAIL_FAST);

        assertEquals(IndexResultStatus.OK, result.getStatus());
        assertSame(iterator, result.getValue());
    }
}
