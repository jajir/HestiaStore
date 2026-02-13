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
    void getSegment_returnsWrappedSegment() {
        assertSame(segment, handler.getSegment());
        assertSame(SegmentHandlerState.READY, handler.getState());
    }

    @Test
    void lock_transitionsToLocked() {
        final SegmentHandlerLockStatus status = handler.lock();

        assertSame(SegmentHandlerLockStatus.OK, status);
        assertSame(SegmentHandlerState.LOCKED, handler.getState());
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

        assertSame(SegmentHandlerState.READY, handler.getState());
        assertSame(segment, handler.getSegment());
    }
}
