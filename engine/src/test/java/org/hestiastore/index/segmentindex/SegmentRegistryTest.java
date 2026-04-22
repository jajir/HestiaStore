package org.hestiastore.index.segmentindex;

import org.hestiastore.index.OperationResult;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentRegistryTest {

    @Mock
    private SegmentHandle<Integer, String> segmentHandle;

    private StubSegmentRegistry<Integer, String> registry;

    @BeforeEach
    void setUp() {
        registry = new StubSegmentRegistry<>(segmentHandle);
    }

    @AfterEach
    void tearDown() {
        registry = null;
    }

    @Test
    void get_remove_close_delegate_to_registry() {
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentHandle<Integer, String> result = registry
                .loadSegment(segmentId);

        assertSame(segmentHandle, result);
        registry.deleteSegment(segmentId);
        registry.close();

        assertSame(segmentId, registry.getLastRemoved());
        assertTrue(registry.wasClosed());
    }

    private static final class StubSegmentRegistry<K, V>
            implements SegmentRegistry<K, V> {

        private final SegmentHandle<K, V> segmentHandle;
        private SegmentId lastRemoved;
        private boolean closed;

        private StubSegmentRegistry(final SegmentHandle<K, V> segmentHandle) {
            this.segmentHandle = segmentHandle;
        }

        @Override
        public SegmentHandle<K, V> loadSegment(final SegmentId segmentId) {
            return segmentHandle;
        }

        @Override
        public Optional<SegmentHandle<K, V>> tryGetSegment(
                final SegmentId segmentId) {
            return OperationResult.ok(segmentHandle);
        }

        @Override
        public SegmentHandle<K, V> createSegment() {
            return segmentHandle;
        }

        @Override
        public void deleteSegment(final SegmentId segmentId) {
            lastRemoved = segmentId;
        }

        @Override
        public boolean deleteSegmentIfAvailable(final SegmentId segmentId) {
            lastRemoved = segmentId;
            return true;
        }

        @Override
        public SegmentRegistryCacheStats metricsSnapshot() {
            return new SegmentRegistryCacheStats(0L, 0L, 0L, 0L, 0, 0);
        }

        @Override
        public boolean updateCacheLimit(final int newLimit) {
            return true;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public SegmentRegistry.Materialization<K, V> materialization() {
            return null;
        }

        @Override
        public SegmentRegistry.Runtime<K, V> runtime() {
            return new SegmentRegistry.Runtime<>() {
                @Override
                public void updateRuntimeLimits(
                        final org.hestiastore.index.segment.SegmentRuntimeLimits runtimeLimits) {
                }

                @Override
                public List<SegmentHandle<K, V>> loadedSegmentsSnapshot() {
                    return List.of();
                }
            };
        }

        private SegmentId getLastRemoved() {
            return lastRemoved;
        }

        private boolean wasClosed() {
            return closed;
        }
    }
}
