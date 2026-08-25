package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SenkuMergeJobTest {

    private TypeDescriptorInteger keys;
    private TypeDescriptorLong values;

    @BeforeEach
    void setUp() {
        keys = new TypeDescriptorInteger();
        values = new TypeDescriptorLong();
    }

    @Test
    void mergesSortedSourcesAndCollapsesDuplicates() {
        final SenkuRunSource first = runCatalogSource(7, 1, 0L,
                Entry.of(1, 10L), Entry.of(3, 30L));
        final SenkuRunSource second = runCatalogSource(7, 1, 1L,
                Entry.of(1, 5L), Entry.of(2, 20L));
        final MemDirectory output = new MemDirectory();

        final SenkuCompletedRun completed = runJob(List.of(first, second),
                output, 11L).execute();

        assertEquals(7, completed.shardId());
        assertEquals(2, completed.level());
        assertEquals(11L, completed.runId());
        assertEquals(List.of(Entry.of(1, 15L), Entry.of(2, 20L),
                Entry.of(3, 30L)), read(output, completed.manifest()));
    }

    @Test
    void oneInputPromotionRewritesIntoNextLevel() {
        final MemDirectory output = new MemDirectory();
        final SenkuRunSource source = runCatalogSource(2, 3, 4L,
                Entry.of(1, 10L));

        final SenkuCompletedRun completed = runJob(List.of(source), output, 5L)
                .execute();

        assertEquals(2, completed.shardId());
        assertEquals(4, completed.level());
        assertEquals(List.of(Entry.of(1, 10L)),
                read(output, completed.manifest()));
    }

    @Test
    void noSourcesPublishesAnEmptyRun() {
        final MemDirectory output = new MemDirectory();

        final SenkuCompletedRun completed = job(List.of(), output, 0, 0, 0L)
                .execute();

        assertEquals(0, completed.manifest().partCount());
        assertEquals(0L, completed.manifest().recordCount());
        assertEquals(List.of(SenkuFileNames.MANIFEST_FILE),
                output.getFileNames().toList());
    }

    @Test
    void publicationFailureFailsWithoutClaimingACompletedRun() {
        final MemDirectory output = new MemDirectory();
        output.touch(SenkuFileNames.partFile(0));

        assertThrows(IndexException.class,
                () -> job(List.of(runSource(Entry.of(1, 1L))), output, 0, 0,
                        0L).execute());
        assertFalse(output.isFileExists(SenkuFileNames.MANIFEST_FILE));
    }

    @Test
    void constructorRejectsNullSourceAndInvalidOutputIdentity() {
        final MemDirectory output = new MemDirectory();
        final List<SenkuMergeSource> nullSource = Arrays.asList(
                (SenkuMergeSource) null);

        assertThrows(IllegalArgumentException.class,
                () -> job(nullSource, output, 0, 0, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> job(List.of(), output, -1, 0, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> job(List.of(), output, 0, -1, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> job(List.of(), output, 0, 0, -1L));
    }

    @Test
    void runFactoryRejectsEmptyMixedAndOverflowingLevels() {
        final MemDirectory output = new MemDirectory();
        final SenkuRunSource shardZero = runCatalogSource(0, 1, 0L,
                Entry.of(1, 1L));
        final SenkuRunSource shardOne = runCatalogSource(1, 1, 0L,
                Entry.of(2, 2L));
        final SenkuRunSource levelTwo = runCatalogSource(0, 2, 0L,
                Entry.of(3, 3L));
        final SenkuRunSource maximumLevel = runCatalogSource(0,
                Integer.MAX_VALUE, 0L, Entry.of(4, 4L));

        assertThrows(IllegalArgumentException.class,
                () -> runJob(List.of(), output, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> runJob(List.of(shardZero, shardOne), output, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> runJob(List.of(shardZero, levelTwo), output, 0L));
        assertThrows(IndexException.class,
                () -> runJob(List.of(maximumLevel), output, 0L));
    }

    private SenkuMergeJob<Integer, Long> job(
            final List<SenkuMergeSource> sources,
            final MemDirectory outputDirectory, final int shardId,
            final int level, final long runId) {
        return new SenkuMergeJob<>(sources, outputDirectory, shardId, level,
                runId, keys, values,
                (key, first, second) -> first + second, 2, 4L,
                DATA_BLOCK_SIZE);
    }

    private SenkuMergeJob<Integer, Long> runJob(
            final List<SenkuRunSource> sources,
            final MemDirectory outputDirectory, final long runId) {
        return SenkuMergeJob.forRuns(sources, outputDirectory, runId, keys,
                values, (key, first, second) -> first + second, 2, 4L,
                DATA_BLOCK_SIZE);
    }

    @SafeVarargs
    private final SenkuMergeSource runSource(
            final Entry<Integer, Long>... entries) {
        return runCatalogSource(0, 0, 0L, entries).mergeSource(DATA_BLOCK_SIZE,
                4L);
    }

    @SafeVarargs
    private final SenkuRunSource runCatalogSource(final int shardId,
            final int level, final long runId,
            final Entry<Integer, Long>... entries) {
        final MemDirectory directory = new MemDirectory();
        final SenkuRunManifest manifest = new SenkuRunWriter<>(directory, keys,
                values, 2, 4L, DATA_BLOCK_SIZE)
                .write(new EntryIteratorList<>(List.of(entries)));
        return new SenkuRunSource(directory, shardId, level, runId, manifest);
    }

    private List<Entry<Integer, Long>> read(final MemDirectory directory,
            final SenkuRunManifest manifest) {
        final SenkuSourceEntryIterator<Integer, Long> iterator =
                new SenkuSourceEntryIterator<>(
                        new LargeFile(directory, DATA_BLOCK_SIZE, 4L,
                                manifest.partCount()).openReader(),
                        keys, values, manifest.recordCount(), true);
        final List<Entry<Integer, Long>> entries = new ArrayList<>();
        while (iterator.hasNext()) {
            entries.add(iterator.next());
        }
        iterator.close();
        return entries;
    }
}
