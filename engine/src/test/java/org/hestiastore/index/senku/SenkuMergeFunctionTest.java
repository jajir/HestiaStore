package org.hestiastore.index.senku;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SenkuMergeFunctionTest {

    @Test
    void apply_invokesConfiguredReduction() {
        final SenkuMergeFunction<Integer, Integer> function =
                (key, first, second) -> key + first + second;

        assertEquals(6, function.apply(1, 2, 3));
    }
}
