package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.segment.Segment;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class SegmentRegistryAccessImplTest {

    @Test
    void forStatusCreatesStatusOnlyAccess() {
        final SegmentRegistryAccess<String> access = SegmentRegistryAccessImpl
                .forStatus(SegmentRegistryResultStatus.BUSY);

        assertSame(SegmentRegistryResultStatus.BUSY, access.getSegmentStatus());
        assertTrue(access.getSegment().isEmpty());
        assertSame(SegmentHandlerLockStatus.BUSY, access.lock());
        assertDoesNotThrow(access::unlock);
    }

    @Test
    void forValueExposesValueOnlyWhenStatusOk() {
        final SegmentRegistryAccess<String> ok = SegmentRegistryAccessImpl
                .forValue(SegmentRegistryResultStatus.OK, "value");
        final SegmentRegistryAccess<String> busy = SegmentRegistryAccessImpl
                .forValue(SegmentRegistryResultStatus.BUSY, "value");

        assertEquals("value", ok.getSegment().orElse(null));
        assertTrue(busy.getSegment().isEmpty());
    }

    @Test
    void forHandlerDelegatesLockAndUnlockToHandler() {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = Mockito.mock(Segment.class);
        final SegmentHandler<Integer, String> handler = new SegmentHandler<>(
                segment);
        final SegmentRegistryAccess<Segment<Integer, String>> access = SegmentRegistryAccessImpl
                .forHandler(SegmentRegistryResultStatus.OK, handler);

        assertSame(segment, access.getSegment().orElse(null));
        assertSame(SegmentHandlerLockStatus.OK, access.lock());
        assertSame(SegmentHandlerLockStatus.BUSY, access.lock());

        access.unlock();

        assertSame(SegmentHandlerLockStatus.OK, access.lock());
        access.unlock();
    }

    @Test
    void forHandlerDoesNotLockWhenStatusIsNotOk() {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = Mockito.mock(Segment.class);
        final SegmentHandler<Integer, String> handler = new SegmentHandler<>(
                segment);
        final SegmentRegistryAccess<Segment<Integer, String>> access = SegmentRegistryAccessImpl
                .forHandler(SegmentRegistryResultStatus.BUSY, handler);

        assertSame(SegmentHandlerLockStatus.BUSY, access.lock());
        assertSame(SegmentHandlerState.READY, handler.getState());
    }

    @Test
    void forStatusClosedAndErrorRemainNonLocking() {
        final SegmentRegistryAccess<String> closed = SegmentRegistryAccessImpl
                .forStatus(SegmentRegistryResultStatus.CLOSED);
        final SegmentRegistryAccess<String> error = SegmentRegistryAccessImpl
                .forStatus(SegmentRegistryResultStatus.ERROR);

        assertTrue(closed.getSegment().isEmpty());
        assertTrue(error.getSegment().isEmpty());
        assertSame(SegmentHandlerLockStatus.BUSY, closed.lock());
        assertSame(SegmentHandlerLockStatus.BUSY, error.lock());
    }

}
