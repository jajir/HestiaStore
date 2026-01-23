package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.junit.jupiter.api.Test;

class SegmentTest {

    @Test
    void openIteratorDefaultsToFailFastIsolation() {
        final StubSegment segment = new StubSegment();
        try {
            segment.openIterator();

            assertEquals(SegmentIteratorIsolation.FAIL_FAST,
                    segment.getLastIsolation());
        } finally {
            segment.close();
        }
    }

    private static final class StubSegment
            implements Segment<Integer, String> {

        private SegmentIteratorIsolation lastIsolation;
        private SegmentState state = SegmentState.READY;

        @Override
        public SegmentStats getStats() {
            return new SegmentStats(0, 0, 0);
        }

        @Override
        public SegmentResult<Void> compact() {
            return SegmentResult.ok();
        }

        @Override
        public Integer checkAndRepairConsistency() {
            return null;
        }

        @Override
        public void invalidateIterators() {
            // no-op
        }

        @Override
        public SegmentResult<EntryIterator<Integer, String>> openIterator(
                final SegmentIteratorIsolation isolation) {
            this.lastIsolation = isolation;
            return SegmentResult.ok(EntryIterator
                    .make(List.<Entry<Integer, String>>of().iterator()));
        }

        @Override
        public SegmentResult<Void> put(final Integer key, final String value) {
            return SegmentResult.ok();
        }

        @Override
        public SegmentResult<Void> flush() {
            return SegmentResult.ok();
        }

        @Override
        public int getNumberOfKeysInWriteCache() {
            return 0;
        }

        @Override
        public long getNumberOfKeysInCache() {
            return 0L;
        }

        @Override
        public long getNumberOfKeysInSegmentCache() {
            return 0L;
        }

        @Override
        public long getNumberOfKeys() {
            return 0L;
        }

        @Override
        public SegmentResult<String> get(final Integer key) {
            return SegmentResult.ok();
        }

        @Override
        public SegmentId getId() {
            return SegmentId.of(1);
        }

        SegmentIteratorIsolation getLastIsolation() {
            return lastIsolation;
        }

        @Override
        public SegmentResult<Void> close() {
            state = SegmentState.CLOSED;
            return SegmentResult.ok();
        }

        @Override
        public SegmentState getState() {
            return state;
        }
    }
}
