package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertSame;

import org.hestiastore.index.segment.Segment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentHandlerTest {

    @Mock
    private Segment<Integer, String> segment;

    private SegmentHandler<Integer, String> handler;

    @BeforeEach
    void setUp() {
        handler = new SegmentHandler<>(segment);
    }

    @AfterEach
    void tearDown() {
        handler = null;
    }

    @Test
    void getSegmentIfReady_returnsOkWhenReady() {
        final SegmentRegistryResult<Segment<Integer, String>> result = handler
                .getSegmentIfReady();

        assertSame(SegmentRegistryResultStatus.OK, result.getStatus());
        assertSame(segment, result.getValue());
    }

    @Test
    void lock_transitionsToLocked() {
        final SegmentHandlerLockStatus status = handler.lock();

        assertSame(SegmentHandlerLockStatus.OK, status);
        assertSame(SegmentHandlerState.LOCKED, handler.getState());

        final SegmentRegistryResult<Segment<Integer, String>> result = handler
                .getSegmentIfReady();
        assertSame(SegmentRegistryResultStatus.BUSY, result.getStatus());
    }

    @Test
    void getSegmentIfReady_returnsBusyWhenLocked() {
        assertSame(SegmentHandlerLockStatus.OK, handler.lock());

        final SegmentRegistryResult<Segment<Integer, String>> result = handler
                .getSegmentIfReady();

        assertSame(SegmentRegistryResultStatus.BUSY, result.getStatus());
    }

    @Test
    void lock_returnsBusyWhenLocked() {
        assertSame(SegmentHandlerLockStatus.OK, handler.lock());

        assertSame(SegmentHandlerLockStatus.BUSY, handler.lock());
    }

    @Test
    void unlock_restoresOk() {
        assertSame(SegmentHandlerLockStatus.OK, handler.lock());

        handler.unlock();

        final SegmentRegistryResult<Segment<Integer, String>> result = handler
                .getSegmentIfReady();
        assertSame(SegmentRegistryResultStatus.OK, result.getStatus());
        assertSame(segment, result.getValue());
        assertSame(SegmentHandlerState.READY, handler.getState());
    }
}
