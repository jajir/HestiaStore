package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.never;
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
    private KeyToSegmentMap<String> keyToSegmentMap;
    private KeyToSegmentMapSynchronizedAdapter<String> synchronizedKeyToSegmentMap;
    private SegmentIndexCore<String, String> core;

    @BeforeEach
    void setUp() {
        asyncDirectory = AsyncDirectoryAdapter.wrap(new MemDirectory());
        keyToSegmentMap = new KeyToSegmentMap<>(asyncDirectory,
                new TypeDescriptorShortString());
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
        core = new SegmentIndexCore<>(synchronizedKeyToSegmentMap,
                segmentRegistry, maintenanceCoordinator);
    }

    @AfterEach
    void tearDown() {
        if (synchronizedKeyToSegmentMap != null
                && !synchronizedKeyToSegmentMap.wasClosed()) {
            synchronizedKeyToSegmentMap.close();
        }
        if (asyncDirectory != null && !asyncDirectory.wasClosed()) {
            asyncDirectory.close();
        }
        core = null;
        keyToSegmentMap = null;
        synchronizedKeyToSegmentMap = null;
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
        final SegmentId segmentId = keyToSegmentMap.insertKeyToSegment("key");
        when(segmentRegistry.getSegment(segmentId))
                .thenReturn(SegmentRegistryResult.ok(segment));
        when(segment.get("key")).thenReturn(SegmentResult.busy());

        final IndexResult<String> result = core.get("key");

        assertEquals(IndexResultStatus.BUSY, result.getStatus());
    }

    @Test
    void get_returnsBusyWhenMappingChangesDuringRead() {
        final SegmentId segmentId = keyToSegmentMap.insertKeyToSegment("key");
        when(segmentRegistry.getSegment(segmentId)).thenAnswer(invocation -> {
            synchronizedKeyToSegmentMap.updateSegmentMaxKey(segmentId, "key-2");
            return SegmentRegistryResult.ok(segment);
        });
        when(segment.get("key")).thenReturn(SegmentResult.ok("value"));

        final IndexResult<String> result = core.get("key");

        assertEquals(IndexResultStatus.BUSY, result.getStatus());
    }

    @Test
    void put_schedulesMaintenanceOnSuccess() {
        final SegmentId segmentId = keyToSegmentMap.insertKeyToSegment("key");
        final KeyToSegmentMap.Snapshot<String> snapshot = synchronizedKeyToSegmentMap
                .snapshot();
        when(segmentRegistry.getSegment(segmentId))
                .thenReturn(SegmentRegistryResult.ok(segment));
        when(segment.put("key", "value")).thenReturn(SegmentResult.ok());

        final IndexResult<Void> result = core.put("key", "value");

        assertEquals(IndexResultStatus.OK, result.getStatus());
        verify(maintenanceCoordinator).handlePostWrite(segment, "key",
                segmentId, snapshot.version());
    }

    @Test
    void put_returnsBusyWhenMappingChangesBeforeWrite() {
        final SegmentId segmentId = keyToSegmentMap.insertKeyToSegment("key");
        when(segmentRegistry.getSegment(segmentId)).thenAnswer(invocation -> {
            synchronizedKeyToSegmentMap.updateSegmentMaxKey(segmentId, "key-2");
            return SegmentRegistryResult.ok(segment);
        });

        final IndexResult<Void> result = core.put("key", "value");

        assertEquals(IndexResultStatus.BUSY, result.getStatus());
        verify(segment, never()).put("key", "value");
        verifyNoInteractions(maintenanceCoordinator);
    }

    @Test
    void put_returnsBusyWhenSegmentIsBusy() {
        final SegmentId segmentId = keyToSegmentMap.insertKeyToSegment("key");
        when(segmentRegistry.getSegment(segmentId))
                .thenReturn(SegmentRegistryResult.ok(segment));
        when(segment.put("key", "value")).thenReturn(SegmentResult.busy());

        final IndexResult<Void> result = core.put("key", "value");

        assertEquals(IndexResultStatus.BUSY, result.getStatus());
        verifyNoInteractions(maintenanceCoordinator);
    }

    @Test
    void openIterator_returnsValueWhenOk() {
        final SegmentId segmentId = keyToSegmentMap.insertKeyToSegment("key");
        final EntryIterator<String, String> iterator = EntryIterator
                .make(List.<Entry<String, String>>of().iterator());
        when(segmentRegistry.getSegment(segmentId))
                .thenReturn(SegmentRegistryResult.ok(segment));
        when(segment.openIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(SegmentResult.ok(iterator));

        final IndexResult<EntryIterator<String, String>> result = core
                .openIterator(segmentId, SegmentIteratorIsolation.FAIL_FAST);

        assertEquals(IndexResultStatus.OK, result.getStatus());
        assertSame(iterator, result.getValue());
    }
}
