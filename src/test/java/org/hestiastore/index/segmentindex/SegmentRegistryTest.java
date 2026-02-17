package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
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
        final Segment<Integer, String> result = registry.getSegment(segmentId)
                .getValue();

        assertSame(segment, result);
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
        public SegmentRegistryResult<Segment<K, V>> getSegment(
                final SegmentId segmentId) {
            return SegmentRegistryResult.ok(segment);
        }

        @Override
        public SegmentRegistryResult<SegmentId> allocateSegmentId() {
            return SegmentRegistryResult.ok(SegmentId.of(1));
        }

        @Override
        public SegmentRegistryResult<Void> deleteSegment(
                final SegmentId segmentId) {
            lastRemoved = segmentId;
            return SegmentRegistryResult.ok();
        }

        @Override
        public SegmentRegistryResult<Void> close() {
            closed = true;
            return SegmentRegistryResult.ok();
        }

        private SegmentId getLastRemoved() {
            return lastRemoved;
        }

        private boolean wasClosed() {
            return closed;
        }
    }
}
