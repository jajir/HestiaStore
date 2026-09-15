package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;
import org.openjdk.jmh.infra.Blackhole;

class SenkuIngestionRotationBenchmarkTest {

    private static final String BLACKHOLE_CHALLENGE =
            "Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.";

    private final SenkuIngestionRotationBenchmark benchmark =
            new SenkuIngestionRotationBenchmark();

    @Test
    void rotationBookkeepingSupportsEveryCandidateStripeCount() {
        final Blackhole blackhole = new Blackhole(BLACKHOLE_CHALLENGE);

        assertDoesNotThrow(() -> rotate(32, blackhole));
        assertDoesNotThrow(() -> rotate(64, blackhole));
        assertDoesNotThrow(() -> rotate(128, blackhole));
    }

    private void rotate(final int stripeCount, final Blackhole blackhole) {
        final SenkuIngestionRotationBenchmark.RotationState state =
                new SenkuIngestionRotationBenchmark.RotationState();
        state.stripeCount = stripeCount;
        state.setup();
        benchmark.rotateBatch(state, blackhole);
    }
}
