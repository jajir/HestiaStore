package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.OperationResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StableSegmentGatewayTest {

    @Mock
    private SegmentRegistry<String, String> segmentRegistry;

    @Mock
    private SegmentHandle<String, String> segmentHandle;

    private Directory asyncDirectory;
    private KeyToSegmentMapImpl<String> keyToSegmentMap;
    private KeyToSegmentMap<String> synchronizedKeyToSegmentMap;
    private StableSegmentGateway<String, String> stableSegmentGateway;

    @BeforeEach
    void setUp() {
        asyncDirectory = new MemDirectory();
        keyToSegmentMap = new KeyToSegmentMapImpl<>(asyncDirectory,
                new TypeDescriptorShortString());
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
        stableSegmentGateway = new StableSegmentGateway<>(
                synchronizedKeyToSegmentMap,
                segmentRegistry);
    }

    @AfterEach
    void tearDown() {
        if (synchronizedKeyToSegmentMap != null
                && !synchronizedKeyToSegmentMap.wasClosed()) {
            synchronizedKeyToSegmentMap.close();
        }
        stableSegmentGateway = null;
        keyToSegmentMap = null;
        synchronizedKeyToSegmentMap = null;
        asyncDirectory = null;
    }

    @Test
    void get_returnsOkNullWhenNoSegmentMapping() {
        final OperationResult<String> result = stableSegmentGateway.get("missing");

        assertEquals(OperationStatus.OK, result.getStatus());
        assertNull(result.getValue());
        verifyNoInteractions(segmentRegistry);
    }

    @Test
    void get_returnsBusyWhenSegmentIsBusy() {
        final SegmentId segmentId = createBootstrapSegment("key");
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.tryGet("key")).thenReturn(OperationResult.busy());

        final OperationResult<String> result = stableSegmentGateway.get("key");

        assertEquals(OperationStatus.BUSY, result.getStatus());
    }

    @Test
    void get_returnsBusyWhenMappingChangesDuringRead() {
        final SegmentId segmentId = createBootstrapSegment("key");
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.tryGet("key")).thenAnswer(invocation -> {
            synchronizedKeyToSegmentMap.extendMaxKeyIfNeeded("key-2");
            return OperationResult.ok("value");
        });

        final OperationResult<String> result = stableSegmentGateway.get("key");

        assertEquals(OperationStatus.BUSY, result.getStatus());
    }

    @Test
    void get_returnsOkWhenMappingAndTopologyStayStable() {
        final SegmentId segmentId = createBootstrapSegment("key");
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.tryGet("key")).thenReturn(OperationResult.ok("value"));

        final OperationResult<String> result = stableSegmentGateway.get("key");

        assertEquals(OperationStatus.OK, result.getStatus());
        assertEquals("value", result.getValue());
    }

    @Test
    void put_returnsOkWhenSegmentAcceptsWrite() {
        final SegmentId segmentId = createBootstrapSegment("key");
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.tryPut("key", "value"))
                .thenReturn(OperationResult.ok());

        final OperationResult<Void> result = stableSegmentGateway.put(segmentId,
                "key", "value");

        assertEquals(OperationStatus.OK, result.getStatus());
    }

    @Test
    void put_returnsBusyWhenSegmentRejectsWrite() {
        final SegmentId segmentId = createBootstrapSegment("key");
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.tryPut("key", "value"))
                .thenReturn(OperationResult.busy());

        final OperationResult<Void> result = stableSegmentGateway.put(segmentId,
                "key", "value");

        assertEquals(OperationStatus.BUSY, result.getStatus());
    }

    @Test
    void put_returnsBusyWhenSegmentHandleIsNotLoaded() {
        final SegmentId segmentId = createBootstrapSegment("key");
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.empty());

        final OperationResult<Void> result = stableSegmentGateway.put(segmentId,
                "key", "value");

        assertEquals(OperationStatus.BUSY, result.getStatus());
    }

    @Test
    void openIterator_returnsValueWhenOk() {
        final SegmentId segmentId = createBootstrapSegment("key");
        final EntryIterator<String, String> iterator = EntryIterator
                .make(List.<Entry<String, String>>of().iterator());
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.tryOpenIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(OperationResult.ok(iterator));

        final OperationResult<EntryIterator<String, String>> result = stableSegmentGateway
                .openIterator(segmentId, SegmentIteratorIsolation.FAIL_FAST);

        assertEquals(OperationStatus.OK, result.getStatus());
        assertSame(iterator, result.getValue());
    }

    @Test
    void flush_mapsClosedStatusWithoutLosingSignal() {
        final SegmentId segmentId = createBootstrapSegment("key");
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.tryFlush()).thenReturn(OperationResult.closed());

        final OperationResult<SegmentHandle<String, String>> result =
                stableSegmentGateway.flush(segmentId);

        assertEquals(OperationStatus.CLOSED, result.getStatus());
    }

    private SegmentId createBootstrapSegment(final String key) {
        synchronizedKeyToSegmentMap.extendMaxKeyIfNeeded(key);
        return synchronizedKeyToSegmentMap.findSegmentIdForKey(key);
    }
}
