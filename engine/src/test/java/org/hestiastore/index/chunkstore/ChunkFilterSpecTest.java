package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

class ChunkFilterSpecTest {

    @Test
    void withParameterReturnsExtendedCopyAndKeepsOriginalImmutable() {
        final ChunkFilterSpec original = ChunkFilterSpec.ofProvider("crypto");
        final ChunkFilterSpec updated = original.withParameter("keyRef",
                "orders-main");

        assertTrue(original.getParameters().isEmpty());
        assertEquals("crypto", updated.getProviderId());
        assertEquals("orders-main", updated.getRequiredParameter("keyRef"));
    }

    @Test
    void parametersMapIsImmutable() {
        final ChunkFilterSpec spec = ChunkFilterSpec.ofProvider("crypto")
                .withParameter("keyRef", "orders-main");

        assertThrows(UnsupportedOperationException.class,
                () -> spec.getParameters().put("region", "eu-central"));
    }

    @Test
    void getRequiredParameterRejectsMissingValue() {
        final ChunkFilterSpec spec = ChunkFilterSpec.ofProvider("crypto");

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> spec.getRequiredParameter("keyRef"));

        assertEquals(
                "Missing required chunk filter parameter 'keyRef' for provider 'crypto'",
                exception.getMessage());
    }

    @Test
    void equalsAndHashCodeIgnoreInsertionOrderOfParameters() {
        final ChunkFilterSpec left = ChunkFilterSpec.ofProvider("crypto")
                .withParameter("keyRef", "orders-main")
                .withParameter("tagBits", "128");
        final ChunkFilterSpec right = ChunkFilterSpec.ofProvider("crypto")
                .withParameter("tagBits", "128")
                .withParameter("keyRef", "orders-main");

        assertEquals(left, right);
        assertEquals(left.hashCode(), right.hashCode());
        assertEquals(Map.of("keyRef", "orders-main", "tagBits", "128"),
                left.getParameters());
    }
}
