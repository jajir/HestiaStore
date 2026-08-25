package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class SenkuRunManifestTest {

    @Test
    void constructor_acceptsEmptyAndNonEmptyRuns() {
        final SenkuRunManifest empty = new SenkuRunManifest(0, 0L);
        final SenkuRunManifest nonEmpty = new SenkuRunManifest(2, 10L);

        assertEquals(0, empty.partCount());
        assertEquals(0L, empty.recordCount());
        assertEquals(2, nonEmpty.partCount());
        assertEquals(10L, nonEmpty.recordCount());
    }

    @Test
    void constructor_rejectsContradictoryCounts() {
        assertThrows(IndexException.class,
                () -> new SenkuRunManifest(0, 1L));
        assertThrows(IndexException.class,
                () -> new SenkuRunManifest(1, 0L));
    }

    @Test
    void constructor_rejectsNegativeCounts() {
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuRunManifest(-1, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuRunManifest(0, -1L));
    }
}
