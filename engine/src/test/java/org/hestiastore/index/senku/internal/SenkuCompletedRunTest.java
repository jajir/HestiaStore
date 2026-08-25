package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class SenkuCompletedRunTest {

    @Test
    void exposesValidatedIdentityAndManifest() {
        final SenkuRunManifest manifest = new SenkuRunManifest(2, 10L);
        final SenkuCompletedRun completed = new SenkuCompletedRun(3, 4, 5L,
                manifest);

        assertEquals(3, completed.shardId());
        assertEquals(4, completed.level());
        assertEquals(5L, completed.runId());
        assertSame(manifest, completed.manifest());
    }

    @Test
    void rejectsInvalidValues() {
        final SenkuRunManifest manifest = new SenkuRunManifest(0, 0L);

        assertThrows(IllegalArgumentException.class,
                () -> new SenkuCompletedRun(-1, 0, 0L, manifest));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuCompletedRun(0, -1, 0L, manifest));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuCompletedRun(0, 0, -1L, manifest));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuCompletedRun(0, 0, 0L, null));
    }
}
