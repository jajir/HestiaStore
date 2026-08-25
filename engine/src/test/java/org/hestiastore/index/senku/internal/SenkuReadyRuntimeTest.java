package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SenkuReadyRuntimeTest {

    private MemDirectory root;
    private TypeDescriptorInteger keys;
    private TypeDescriptorLong values;
    private FileLock fileLock;

    @BeforeEach
    void setUp() {
        root = new MemDirectory();
        keys = new TypeDescriptorInteger();
        values = new TypeDescriptorLong();
        fileLock = root.getLock(SenkuFileNames.LOCK_FILE);
        fileLock.lock();
        root.mkdir(SenkuFileNames.FLUSH_DIRECTORY);
    }

    @Test
    void globallyMergesTerminalShardRunsLazily() {
        writeRun(0, 2, 7L, Entry.of(1, 10L), Entry.of(4, 40L));
        writeRun(1, 1, 3L, Entry.of(2, 20L), Entry.of(3, 30L));
        SenkuMetadataCodec.publishReady(root, 2);
        final SenkuReadyRuntime<Integer, Long> ready = ready();

        try (Stream<Entry<Integer, Long>> stream = ready.openStream()) {
            assertEquals(List.of(Entry.of(1, 10L), Entry.of(2, 20L),
                    Entry.of(3, 30L), Entry.of(4, 40L)), stream.toList());
        }
        ready.close();

        assertFalse(root.isFileExists(SenkuFileNames.LOCK_FILE));
    }

    @Test
    void emptyTerminalRunsProduceAnEmptyStream() {
        writeRun(0, 0, 0L);
        writeRun(1, 0, 0L);
        SenkuMetadataCodec.publishReady(root, 2);
        final SenkuReadyRuntime<Integer, Long> ready = ready();

        try (Stream<Entry<Integer, Long>> stream = ready.openStream()) {
            assertEquals(List.of(), stream.toList());
        }
        ready.close();
    }

    @Test
    void permitsOnlyOneActiveStreamAndCloseReleasesTheSlot() {
        writeRun(0, 0, 0L, Entry.of(1, 1L));
        SenkuMetadataCodec.publishReady(root, 1);
        final SenkuReadyRuntime<Integer, Long> ready = ready();
        final Stream<Entry<Integer, Long>> first = ready.openStream();

        assertThrows(IndexException.class, ready::openStream);
        first.close();
        try (Stream<Entry<Integer, Long>> second = ready.openStream()) {
            assertEquals(List.of(Entry.of(1, 1L)), second.toList());
        }
        ready.close();
    }

    @Test
    void readyCloseBreaksActiveStreamAndIsIdempotent() {
        writeRun(0, 0, 0L, Entry.of(1, 1L), Entry.of(2, 2L));
        SenkuMetadataCodec.publishReady(root, 1);
        final SenkuReadyRuntime<Integer, Long> ready = ready();
        final Stream<Entry<Integer, Long>> stream = ready.openStream();

        ready.close();
        ready.close();

        assertEquals(List.of(), stream.toList());
        stream.close();
        assertThrows(IndexException.class, ready::openStream);
    }

    @Test
    void comparatorEqualKeysAcrossShardsFailFast() {
        writeRun(0, 0, 0L, Entry.of(1, 1L));
        writeRun(1, 0, 0L, Entry.of(1, 2L));
        SenkuMetadataCodec.publishReady(root, 2);
        final SenkuReadyRuntime<Integer, Long> ready = ready();

        try (Stream<Entry<Integer, Long>> stream = ready.openStream()) {
            assertThrows(IndexException.class, stream::toList);
        }
        ready.close();
    }

    @Test
    void constructorRejectsMissingExtraAndMalformedReadyLayout() {
        assertThrows(IndexException.class, this::ready);

        writeRun(0, 0, 0L);
        SenkuMetadataCodec.publishReady(root, 1);
        root.touch("unknown");
        assertThrows(IndexException.class, this::ready);
        root.deleteFile("unknown");

        final Directory run = runDirectory(0, 0, 0L);
        run.touch("unexpected.tmp");
        assertThrows(IndexException.class, this::ready);
    }

    @SafeVarargs
    private final void writeRun(final int shardId, final int level,
            final long runId, final Entry<Integer, Long>... entries) {
        final Directory run = createRunDirectory(shardId, level, runId);
        new SenkuRunWriter<>(run, keys, values, 1, 2L, DATA_BLOCK_SIZE)
                .write(new EntryIteratorList<>(List.of(entries)));
    }

    private Directory createRunDirectory(final int shardId, final int level,
            final long runId) {
        final String shardName = SenkuFileNames.shardDirectory(shardId);
        if (!root.isFileExists(shardName)) {
            root.mkdir(shardName);
        }
        final Directory shard = root.openSubDirectory(shardName);
        final String levelName = SenkuFileNames.levelDirectory(level);
        if (!shard.isFileExists(levelName)) {
            shard.mkdir(levelName);
        }
        final Directory levelDirectory = shard.openSubDirectory(levelName);
        final String runName = SenkuFileNames.runDirectory(runId);
        levelDirectory.mkdir(runName);
        return levelDirectory.openSubDirectory(runName);
    }

    private Directory runDirectory(final int shardId, final int level,
            final long runId) {
        return root.openSubDirectory(SenkuFileNames.shardDirectory(shardId))
                .openSubDirectory(SenkuFileNames.levelDirectory(level))
                .openSubDirectory(SenkuFileNames.runDirectory(runId));
    }

    private SenkuReadyRuntime<Integer, Long> ready() {
        return new SenkuReadyRuntime<>(root, keys, values, DATA_BLOCK_SIZE,
                fileLock);
    }
}
