package org.hestiastore.index.segmentindex.core.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.CloseableResource;
import org.junit.jupiter.api.Test;

class CloseFailureAccumulatorTest {

    @Test
    void rethrowIfPresent_usesFirstFailureAndSuppressesLaterOnes() {
        final RuntimeException firstFailure = new RuntimeException("first");
        final RuntimeException secondFailure = new RuntimeException("second");
        final CloseFailureAccumulator accumulator =
                new CloseFailureAccumulator();

        accumulator.close(failingResource(firstFailure));
        accumulator.close(failingResource(secondFailure));

        final RuntimeException thrown = assertThrows(RuntimeException.class,
                accumulator::rethrowIfPresent);

        assertEquals(firstFailure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertEquals(secondFailure, thrown.getSuppressed()[0]);
    }

    @Test
    void closeIfResource_ignoresNonCloseableCandidates() {
        final CloseFailureAccumulator accumulator =
                new CloseFailureAccumulator();

        accumulator.closeIfResource("not-closeable");
        accumulator.rethrowIfPresent();
    }

    private static CloseableResource failingResource(
            final RuntimeException failure) {
        return new CloseableResource() {
            @Override
            public boolean wasClosed() {
                return false;
            }

            @Override
            public void close() {
                throw failure;
            }
        };
    }
}
