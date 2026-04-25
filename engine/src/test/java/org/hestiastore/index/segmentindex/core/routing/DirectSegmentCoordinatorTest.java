package org.hestiastore.index.segmentindex.core.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Function;

import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.segmentaccess.SegmentAccessService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DirectSegmentCoordinatorTest {

    @Mock
    private KeyToSegmentMap<Integer> keyToSegmentMap;

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private SegmentAccessService<Integer, String> segmentAccessService;

    @Mock
    private IndexRetryPolicy retryPolicy;

    @Mock
    private BlockingSegment<Integer, String> blockingSegment;

    private DirectSegmentCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        coordinator = new DirectSegmentCoordinator<>(keyToSegmentMap,
                segmentRegistry, segmentAccessService, retryPolicy);
    }

    @Test
    void getReadsValueThroughSegmentAccessService() {
        when(blockingSegment.get(10)).thenReturn("ten");
        when(segmentAccessService.withSegmentForRead(eq(10),
                anyReadOperation())).thenAnswer(invocation -> {
                    final Function<BlockingSegment<Integer, String>, String> operation =
                            invocation.getArgument(1);
                    return operation.apply(blockingSegment);
                });

        final String result = coordinator.get(10);

        assertEquals("ten", result);
        verify(blockingSegment).get(10);
    }

    @Test
    void getReturnsOkWithNullWhenKeyHasNoSegment() {
        when(segmentAccessService.withSegmentForRead(eq(10),
                anyReadOperation())).thenReturn(null);

        final String result = coordinator.get(10);

        assertNull(result);
    }

    @Test
    void putWritesValueThroughSegmentAccessService() {
        when(segmentAccessService.withSegmentForWrite(eq(11),
                anyWriteOperation())).thenAnswer(invocation -> {
                    final Function<BlockingSegment<Integer, String>, Void> operation =
                            invocation.getArgument(1);
                    return operation.apply(blockingSegment);
                });

        coordinator.put(11, "v11");

        verify(blockingSegment).put(11, "v11");
    }

    private Function<BlockingSegment<Integer, String>, String> anyReadOperation() {
        return any();
    }

    private Function<BlockingSegment<Integer, String>, Void> anyWriteOperation() {
        return any();
    }
}
