package org.hestiastore.index.senku.internal;

import java.util.Optional;
import org.hestiastore.index.senku.SenkuLongKeySummary;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class SenkuRunManifestTest {

    @Test
    void validatesOptionalSummaryAgainstExactRecordCount() {
        final SenkuLongKeySummary summary = SenkuLongKeySummary.of(2,
                new long[] { 4 }, new long[] { 2 });
        final SenkuRunManifest manifest = new SenkuRunManifest(1, 2,
                Optional.of(summary));
        assertEquals(2, manifest.longKeySummary().orElseThrow().recordCount());
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuRunManifest(1, 3, Optional.of(summary)));
    }

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
        assertThrows(IndexException.class, () -> new SenkuRunManifest(0, 1L));
        assertThrows(IndexException.class, () -> new SenkuRunManifest(1, 0L));
    }

    @Test
    void constructor_rejectsNegativeCounts() {
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuRunManifest(-1, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuRunManifest(0, -1L));
    }
}
