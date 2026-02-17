package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

class AbstractChainOfFiltersTest {

    @Test
    void runs_all_filters_and_returns_true_when_no_short_circuit() {
        final var f1 = new RecordingFilter();
        final var f2 = new RecordingFilter();
        final var chain = new TestChain(List.of(f1, f2));

        final boolean result = chain.filter("ctx", new StringBuilder());

        assertTrue(result);
        assertTrue(f1.invoked);
        assertTrue(f2.invoked);
    }

    @Test
    void stops_on_first_false_and_returns_false() {
        final var f1 = new RecordingFilter();
        final var f2 = new RecordingFilter(false);
        final var f3 = new RecordingFilter();
        final var chain = new TestChain(List.of(f1, f2, f3));

        final boolean result = chain.filter("ctx", new StringBuilder());

        assertFalse(result);
        assertTrue(f1.invoked);
        assertTrue(f2.invoked);
        assertFalse(f3.invoked);
    }

    private static final class RecordingFilter implements Filter<String, StringBuilder> {
        boolean invoked = false;
        final boolean outcome;

        RecordingFilter() {
            this(true);
        }

        RecordingFilter(final boolean outcome) {
            this.outcome = outcome;
        }

        @Override
        public boolean filter(final String context, final StringBuilder result) {
            invoked = true;
            result.append(context);
            return outcome;
        }
    }

    private static final class TestChain extends AbstractChainOfFilters<String, StringBuilder> {
        TestChain(final List<Filter<String, StringBuilder>> filters) {
            super(filters);
        }

        @Override
        public boolean filter(final String context, final StringBuilder result) {
            return super.filter(context, result);
        }
    }
}
