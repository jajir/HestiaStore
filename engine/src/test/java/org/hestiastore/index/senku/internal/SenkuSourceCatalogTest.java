package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SenkuSourceCatalogTest {

    @Test
    void reconcileAddsNewSourcesAndRejectsMissingOrChangedKnownSources() {
        final SenkuSourceCatalog catalog = new SenkuSourceCatalog();
        final SenkuRunSource run = run(0, 0, 0L, 1, 2L);

        catalog.reconcile(Map.of(1L, 2), List.of(run), Set.of());
        catalog.reconcile(Map.of(1L, 2), List.of(run), Set.of());

        assertEquals(1, catalog.flushCount());
        assertEquals(1, catalog.runCount());
        assertThrows(IndexException.class,
                () -> catalog.reconcile(Map.of(), List.of(run), Set.of()));
        assertThrows(IndexException.class,
                () -> catalog.reconcile(Map.of(1L, 3), List.of(run), Set.of()));
        assertThrows(IndexException.class,
                () -> catalog.reconcile(Map.of(1L, 2), List.of(), Set.of()));
        assertThrows(IndexException.class,
                () -> catalog.reconcile(Map.of(1L, 2),
                        List.of(run(0, 0, 0L, 2, 2L)), Set.of()));
    }

    @Test
    void reservedPublishedOutputIsNotAddedToActiveCatalog() {
        final SenkuSourceCatalog catalog = new SenkuSourceCatalog();
        final SenkuRunSource output = run(2, 3, 4L, 0, 0L);

        catalog.reconcile(Map.of(), List.of(output),
                Set.of(SenkuSourceCatalog.runPath(output)));

        assertEquals(0, catalog.runCount());
    }

    @Test
    void flushSelectionUsesOldestIdsAndDrainAllowsPartialGroup() {
        final SenkuSourceCatalog catalog = new SenkuSourceCatalog();
        catalog.reconcile(Map.of(7L, 1, 2L, 1, 5L, 1), List.of(),
                Set.of());

        assertArrayEquals(new long[] { 2L, 5L },
                catalog.eligibleFlushIds(2, false, Set.of()));
        assertArrayEquals(new long[0], catalog.eligibleFlushIds(4, false,
                Set.of()));
        assertArrayEquals(new long[] { 5L, 7L },
                catalog.eligibleFlushIds(4, true,
                        Set.of(SenkuSourceCatalog.flushPath(2L))));
    }

    @Test
    void runSelectionIsBottomUpThenShardAndOldestRun() {
        final SenkuSourceCatalog catalog = new SenkuSourceCatalog();
        final SenkuRunSource shardOneOld = run(1, 0, 1L, 0, 0L);
        final SenkuRunSource shardOneNew = run(1, 0, 3L, 0, 0L);
        final SenkuRunSource shardZeroLevelOne = run(0, 1, 0L, 0, 0L);
        final SenkuRunSource shardZeroLevelTwo = run(0, 2, 0L, 0, 0L);
        catalog.reconcile(Map.of(), List.of(shardZeroLevelTwo, shardOneNew,
                shardZeroLevelOne, shardOneOld), Set.of());

        assertEquals(List.of(shardOneOld, shardOneNew),
                catalog.eligibleRunSources(2, false, Set.of(), Set.of()));
        assertEquals(List.of(shardZeroLevelOne), catalog.eligibleRunSources(2,
                true, Set.of(1), Set.of()));
        assertEquals(List.of(), catalog.eligibleRunSources(2, false,
                Set.of(1), Set.of()));
    }

    @Test
    void acceptedL0AndRunMergeReplaceExactCatalogSources() {
        final SenkuSourceCatalog catalog = new SenkuSourceCatalog();
        final SenkuRunSource oldRun = run(0, 0, 0L, 0, 0L);
        catalog.reconcile(Map.of(1L, 1), List.of(oldRun), Set.of());
        final SenkuRunSource l0 = run(1, 0, 0L, 0, 0L);

        catalog.acceptL0(new long[] { 1L }, List.of(l0));

        assertEquals(0, catalog.flushCount());
        assertEquals(2, catalog.runCount());
        final SenkuRunSource replacement = run(0, 1, 0L, 0, 0L);
        catalog.acceptRunMerge(List.of(oldRun), replacement);
        assertEquals(2, catalog.runCount());
        assertEquals(0L, catalog.maximumRunId(0, 1));
        assertEquals(1, catalog.runCount(0));
    }

    @Test
    void acceptanceRejectsUnknownInputsWithoutPartialMutation() {
        final SenkuSourceCatalog catalog = new SenkuSourceCatalog();
        catalog.reconcile(Map.of(1L, 1), List.of(), Set.of());

        assertThrows(IndexException.class,
                () -> catalog.acceptL0(new long[] { 1L, 2L },
                        List.of(run(0, 0, 0L, 0, 0L))));

        assertEquals(1, catalog.flushCount());
        assertEquals(0, catalog.runCount());
    }

    private static SenkuRunSource run(final int shardId, final int level,
            final long runId, final int partCount, final long recordCount) {
        return new SenkuRunSource(new MemDirectory(), shardId, level, runId,
                new SenkuRunManifest(partCount, recordCount));
    }
}
