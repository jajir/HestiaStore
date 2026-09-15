package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.senku.SenkuLongKeySummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SenkuMetadataDiscoveryTest {

    private static final String MANIFEST = SenkuFileNames.MANIFEST_FILE;
    private static final String SHARD = SenkuFileNames.shardDirectory(0);
    private static final String LEVEL = SenkuFileNames.levelDirectory(0);
    private static final String RUN = SenkuFileNames.runDirectory(0);

    @Spy
    private MemDirectory root;
    @Spy
    private MemDirectory shard;
    @Spy
    private MemDirectory level;
    @Spy
    private MemDirectory run;
    @Spy
    private MemDirectory committedFlush;
    @Spy
    private MemDirectory flush;

    private SenkuSourceCatalog catalog;
    private SenkuMetadataDiscovery discovery;

    @BeforeEach
    void setUp() {
        root.mkdir(SenkuFileNames.FLUSH_DIRECTORY);
        root.mkdir(SHARD);
        shard.mkdir(LEVEL);
        level.mkdir(RUN);
        doReturn(shard).when(root).openSubDirectory(SHARD);
        doReturn(level).when(shard).openSubDirectory(LEVEL);
        doReturn(run).when(level).openSubDirectory(RUN);
        catalog = new SenkuSourceCatalog();
        discovery = new SenkuMetadataDiscovery(root, flush, 1);
    }

    @Test
    void cachedDiscoveryReusesManifestIdentityWithoutReadingItAgain() {
        publishSummary(7);
        discovery.reconcile(catalog, Set.of(), true);
        final SenkuRunSource first = selectedRun();

        discovery.reconcile(catalog, Set.of(), false);
        discovery.reconcile(catalog, Set.of(), false);

        assertSame(first, selectedRun());
        verify(run).getFileReader(MANIFEST);
        assertEquals(1, catalog.runCount());
    }

    @Test
    void strictValidationRereadsKnownSummaryAndRejectsChangedContents() {
        publishSummary(7);
        discovery.reconcile(catalog, Set.of(), true);
        discovery.reconcile(catalog, Set.of(), true);
        final SenkuRunSource validated = selectedRun();
        discovery.reconcile(catalog, Set.of(), false);
        assertSame(validated, selectedRun());
        verify(run, times(2)).getFileReader(MANIFEST);
        run.deleteFile(MANIFEST);
        publishSummary(8);

        discovery.reconcile(catalog, Set.of(), false);
        assertThrows(IndexException.class,
                () -> discovery.reconcile(catalog, Set.of(), true));
        verify(run, times(3)).getFileReader(MANIFEST);
    }

    @Test
    void incompletePreviouslyObservedSourcesBecomeVisibleAfterPublication() {
        final Directory incompleteFlush = flush
                .openSubDirectory(SenkuFileNames.flushDirectory(0));
        discovery.reconcile(catalog, Set.of(), false);
        assertEquals(0, catalog.runCount());
        assertEquals(0, catalog.flushCount());
        verify(run, never()).getFileReader(MANIFEST);
        publishSummary(7);
        SenkuMetadataCodec.publishFlushManifest(incompleteFlush, 1);

        discovery.reconcile(catalog, Set.of(), false);

        assertEquals(1, catalog.runCount());
        assertEquals(1, catalog.flushCount());
        verify(run).getFileReader(MANIFEST);
    }

    @Test
    void cachedDiscoveryStillRejectsMissingKnownManifest() {
        publishSummary(7);
        discovery.reconcile(catalog, Set.of(), false);
        run.deleteFile(MANIFEST);

        assertThrows(IndexException.class,
                () -> discovery.reconcile(catalog, Set.of(), false));
    }

    @Test
    void newMalformedPublicationFailsAndIsNotCached() {
        run.touch(MANIFEST);
        assertThrows(IndexException.class,
                () -> discovery.reconcile(catalog, Set.of(), false));
        run.deleteFile(MANIFEST);
        publishSummary(7);

        discovery.reconcile(catalog, Set.of(), false);

        assertEquals(1, catalog.runCount());
        verify(run, times(2)).getFileReader(MANIFEST);
    }

    @Test
    void reservedPublicationIsParsedOnceButNotAcceptedPrematurely() {
        publishSummary(7);
        final String path = SHARD + "/" + LEVEL + "/" + RUN;
        discovery.reconcile(catalog, Set.of(path), false);
        discovery.reconcile(catalog, Set.of(path), false);
        assertEquals(0, catalog.runCount());

        discovery.reconcile(catalog, Set.of(), false);

        assertEquals(1, catalog.runCount());
        verify(run).getFileReader(MANIFEST);
    }

    @Test
    void acceptedOutputUsesExistingManifestAndDeletedInputsAreEvicted() {
        final Directory inputFlush = flush
                .openSubDirectory(SenkuFileNames.flushDirectory(0));
        SenkuMetadataCodec.publishFlushManifest(inputFlush, 1);
        discovery.reconcile(catalog, Set.of(), false);
        final SenkuRunManifest manifest = summary(7);
        SenkuMetadataCodec.publishRunManifest(run, manifest);
        final SenkuRunSource output = new SenkuRunSource(run, 0, 0, 0,
                manifest);
        catalog.acceptL0(new long[] { 0 }, List.of(output));
        inputFlush.deleteFile(MANIFEST);
        flush.rmdir(SenkuFileNames.flushDirectory(0));
        discovery.forgetFlush(0);
        discovery.rememberRun(output);

        discovery.reconcile(catalog, Set.of(), false);

        assertSame(output, selectedRun());
        verify(run, never()).getFileReader(MANIFEST);
        discovery.forgetRun(output);
        discovery.reconcile(catalog, Set.of(), false);
        verify(run).getFileReader(MANIFEST);
        assertEquals(0, catalog.flushCount());
    }

    @Test
    void discoveryRejectsMalformedAndOutOfRangeStructure() {
        discovery.reconcile(catalog, Set.of(), false);
        root.mkdir("unexpected");
        assertThrows(IndexException.class,
                () -> discovery.reconcile(catalog, Set.of(), false));
        root.rmdir("unexpected");
        root.mkdir(SenkuFileNames.shardDirectory(1));
        assertThrows(IndexException.class,
                () -> discovery.reconcile(catalog, Set.of(), false));
    }

    @Test
    void knownFlushIsReadOnceWhileWritingAndRevalidatedStrictly() {
        final String name = SenkuFileNames.flushDirectory(0);
        flush.mkdir(name);
        doReturn(committedFlush).when(flush).openSubDirectory(name);
        SenkuMetadataCodec.publishFlushManifest(committedFlush, 1);
        discovery.reconcile(catalog, Set.of(), false);
        discovery.reconcile(catalog, Set.of(), false);
        verify(committedFlush).getFileReader(MANIFEST);
        committedFlush.deleteFile(MANIFEST);
        SenkuMetadataCodec.publishFlushManifest(committedFlush, 2);
        assertThrows(IndexException.class,
                () -> discovery.reconcile(catalog, Set.of(), true));
        verify(committedFlush, times(2)).getFileReader(MANIFEST);
    }

    private SenkuRunSource selectedRun() {
        return catalog.eligibleRunSources(1, false, Set.of(), Set.of()).get(0);
    }

    private void publishSummary(final long key) {
        SenkuMetadataCodec.publishRunManifest(run, summary(key));
    }

    private static SenkuRunManifest summary(final long key) {
        return new SenkuRunManifest(1, 2, Optional.of(SenkuLongKeySummary.of(2,
                new long[] { key }, new long[] { 2 })));
    }
}
