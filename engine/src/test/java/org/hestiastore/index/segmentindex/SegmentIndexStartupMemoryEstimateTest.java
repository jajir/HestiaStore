package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

class SegmentIndexStartupMemoryEstimateTest {

    @Test
    @SuppressWarnings("unchecked")
    void defaultStartupMemoryEstimateIsIncomplete() {
        final SegmentIndex<Integer, String> index =
                mock(SegmentIndex.class, CALLS_REAL_METHODS);

        final MemoryEstimateReport report = index.startupMemoryEstimate();

        assertFalse(report.isComplete());
        assertTrue(report.totalEstimatedBytes().isEmpty());
        assertTrue(report.text().contains("unavailable"));
    }
}
