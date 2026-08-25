package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

class SenkuWritingStateTest {

    @Test
    void containsOnlyDocumentedInternalLifecycleStates() {
        assertEquals(List.of("WRITING", "FINISHING", "TRANSFERRED", "ERROR"),
                Arrays.stream(SenkuWritingState.values())
                        .map(Enum::name).toList());
    }
}
