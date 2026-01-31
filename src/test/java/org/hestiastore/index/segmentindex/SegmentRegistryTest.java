package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentRegistryAccess;
import org.hestiastore.index.segmentregistry.SegmentRegistryAccessImpl;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentRegistryTest {

    @Mock
    private Segment<Integer, String> segment;

    private StubSegmentRegistry<Integer, String> registry;

    @BeforeEach
    void setUp() {
        registry = new StubSegmentRegistry<>(segment);
    }

    @AfterEach
    void tearDown() {
        registry = null;
    }

    @Test
    void get_remove_close_delegate_to_registry() {
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentRegistryAccess<Segment<Integer, String>> result = registry
                .getSegment(segmentId);

        assertSame(segment, result.getSegment().orElse(null));
        registry.deleteSegment(segmentId);
        registry.close();

        assertSame(segmentId, registry.getLastRemoved());
        assertTrue(registry.wasClosed());
    }

    private static final class StubSegmentRegistry<K, V>
            implements SegmentRegistry<K, V> {

        private final Segment<K, V> segment;
        private SegmentId lastRemoved;
        private boolean closed;

        private StubSegmentRegistry(final Segment<K, V> segment) {
            this.segment = segment;
        }

        @Override
        public SegmentRegistryAccess<Segment<K, V>> getSegment(
                final SegmentId segmentId) {
            return SegmentRegistryAccessImpl
                    .forValue(SegmentRegistryResultStatus.OK, segment);
        }

        @Override
        public SegmentRegistryAccess<SegmentId> allocateSegmentId() {
            return SegmentRegistryAccessImpl.forValue(
                    SegmentRegistryResultStatus.OK, SegmentId.of(1));
        }

        @Override
        public SegmentRegistryAccess<Void> deleteSegment(
                final SegmentId segmentId) {
            lastRemoved = segmentId;
            return SegmentRegistryAccessImpl
                    .forStatus(SegmentRegistryResultStatus.OK);
        }

        @Override
        public SegmentRegistryAccess<Void> close() {
            closed = true;
            return SegmentRegistryAccessImpl
                    .forStatus(SegmentRegistryResultStatus.OK);
        }

        private SegmentId getLastRemoved() {
            return lastRemoved;
        }

        private boolean wasClosed() {
            return closed;
        }
    }
}
