package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class SplitFailureReporterTest {

    @Test
    void noOpIgnoresFailure() {
        final SplitFailureReporter reporter = SplitFailureReporter.noOp();

        assertDoesNotThrow(() -> reporter.reportFailure(
                new IndexException("simulated split failure")));
    }
}
