package org.hestiastore.index.segmentindex.split;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PartitionSplitPolicyTest {

    @Test
    void estimateNumberOfKeys_returnsConfiguredValue() {
        final PartitionSplitPolicy policy = new PartitionSplitPolicy(57L);
        assertEquals(57L, policy.estimateNumberOfKeys());
    }

    @Test
    void rejects_negative_estimated_keys() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> new PartitionSplitPolicy(-1L));
        assertEquals(
                "Property 'estimatedNumberOfKeys' must be >= 0.",
                err.getMessage());
    }
}
