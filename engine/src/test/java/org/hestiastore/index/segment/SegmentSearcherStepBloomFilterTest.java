package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSearcherStepBloomFilterTest {

    @Mock
    private SegmentSearcherContext<String, Long> ctx;

    private SegmentSearcherStepBloomFilter<String, Long> step;

    @BeforeEach
    void setup() {
        step = new SegmentSearcherStepBloomFilter<>();
    }

    @Test
    void short_circuits_when_not_stored() {
        when(ctx.isNotStoredInBloomFilter()).thenReturn(true);
        final var res = new SegmentSearcherResult<Long>();

        final boolean cont = step.filter(ctx, res);

        assertFalse(cont);
        assertNull(res.getValue());
    }

    @Test
    void continues_when_maybe_present() {
        when(ctx.isNotStoredInBloomFilter()).thenReturn(false);
        final var res = new SegmentSearcherResult<Long>();

        final boolean cont = step.filter(ctx, res);

        assertTrue(cont);
        assertNull(res.getValue());
    }
}
