package org.hestiastore.index.segment;

import org.hestiastore.index.OperationResult;
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
        public OperationResult<Void> compact() {
            return OperationResult.ok();
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
        public OperationResult<EntryIterator<Integer, String>> openIterator(
                final SegmentIteratorIsolation isolation) {
            this.lastIsolation = isolation;
            return OperationResult.ok(EntryIterator
                    .make(List.<Entry<Integer, String>>of().iterator()));
        }

        @Override
        public OperationResult<Void> put(final Integer key, final String value) {
            return OperationResult.ok();
        }

        @Override
        public OperationResult<Void> flush() {
            return OperationResult.ok();
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
        public int getNumberOfDeltaCacheFiles() {
            return 0;
        }

        @Override
        public long getNumberOfKeys() {
            return 0L;
        }

        @Override
        public OperationResult<String> get(final Integer key) {
            return OperationResult.ok();
        }

        @Override
        public SegmentId getId() {
            return SegmentId.of(1);
        }

        SegmentIteratorIsolation getLastIsolation() {
            return lastIsolation;
        }

        @Override
        public OperationResult<Void> close() {
            state = SegmentState.CLOSED;
            return OperationResult.ok();
        }

        @Override
        public SegmentState getState() {
            return state;
        }
    }
}
