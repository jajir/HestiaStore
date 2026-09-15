package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.hestiastore.index.senku.SenkuLongKeySummary;
import org.junit.jupiter.api.Test;

class SenkuLongKeySamplerTest {

    @Test
    void selectsOnlyBoundedThinnedKeysAndAccountsForPartialFinalBucket() {
        final SenkuLongKeySampler sampler = new SenkuLongKeySampler();
        assertEquals(0, sampler.snapshot().recordCount());
        int selected = 0;
        for (long key = 0; key < 100_003; key++) {
            if (sampler.selectNext()) {
                sampler.addSelectedKey(key * 3);
                selected++;
            }
        }
        final SenkuLongKeySummary summary = sampler.snapshot();
        assertEquals(100_003, summary.recordCount());
        assertEquals(100_003, Arrays.stream(summary.weights()).sum());
        assertTrue(summary.keys().length <= 256);
        assertTrue(selected < 4096,
                "Unselected encoded keys need no decoding.");
        assertEquals(0, summary.keys()[0]);
        assertEquals(512, summary.weights()[0]);
        assertEquals(163, summary.weights()[summary.weights().length - 1]);
        assertArrayEquals(summary.keys(), sampler.snapshot().keys());
    }

    @Test
    void rejectsProtocolMisuseAndNonIncreasingSelectedKeys() {
        final SenkuLongKeySampler sampler = new SenkuLongKeySampler();
        assertThrows(IllegalArgumentException.class,
                () -> sampler.addSelectedKey(1));
        assertTrue(sampler.selectNext());
        assertThrows(IllegalArgumentException.class, sampler::selectNext);
        assertThrows(IllegalArgumentException.class, sampler::snapshot);
        sampler.addSelectedKey(1);
        assertTrue(sampler.selectNext());
        assertThrows(IllegalArgumentException.class,
                () -> sampler.addSelectedKey(1));
    }
}
