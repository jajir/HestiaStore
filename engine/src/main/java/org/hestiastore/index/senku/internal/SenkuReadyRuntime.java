package org.hestiastore.index.senku.internal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.senku.SenkuReady;

/**
 * Exclusive no-background-thread ready-index implementation.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SenkuReadyRuntime<K, V> implements SenkuReady<K, V> {

    private final Directory rootDirectory;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final DataBlockSize dataBlockSize;
    private final FileLock fileLock;
    private final List<SenkuRunSource> terminalRuns;
    private final ReentrantLock lock = new ReentrantLock();

    private SenkuMergedEntryIterator<K, V> activeIterator;
    private boolean closed;
    private boolean lockReleased;

    /**
     * Validates and owns an already-locked ready index.
     *
     * @param rootDirectory Senku root
     * @param keyTypeDescriptor key codec and comparator
     * @param valueTypeDescriptor value codec
     * @param dataBlockSize chunk-store block size
     * @param fileLock already-held root lock
     */
    SenkuReadyRuntime(final Directory rootDirectory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final DataBlockSize dataBlockSize, final FileLock fileLock) {
        this.rootDirectory = Vldtn.requireNonNull(rootDirectory,
                "rootDirectory");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
        this.fileLock = Vldtn.requireNonNull(fileLock, "fileLock");
        terminalRuns = validateReadyLayout();
    }

    @Override
    public Stream<Entry<K, V>> openStream() {
        lock.lock();
        try {
            ensureOpen();
            if (activeIterator != null) {
                throw new IndexException("A Senku ready stream is already open.");
            }
            final List<EntryIterator<K, V>> inputs = openInputs();
            try {
                activeIterator = new SenkuMergedEntryIterator<>(inputs,
                        keyTypeDescriptor.getComparator(),
                        (key, first, second) -> {
                            throw new IndexException(
                                    "Comparator-equal terminal keys occurred in multiple shards.");
                        });
            } catch (Exception e) {
                closeInputs(inputs, e);
                throw e;
            }
            final SenkuMergedEntryIterator<K, V> iterator = activeIterator;
            return StreamSupport.stream(new SenkuStreamSpliterator<>(iterator,
                    keyTypeDescriptor.getComparator()), false)
                    .onClose(() -> closeStream(iterator));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (closed && lockReleased) {
                return;
            }
            IndexException failure = closeActiveIterator();
            if (!lockReleased) {
                try {
                    fileLock.unlock();
                    lockReleased = true;
                } catch (Exception e) {
                    failure = appendFailure(failure,
                            "Unable to release Senku ready lock.", e);
                }
            }
            closed = lockReleased;
            if (failure != null) {
                throw failure;
            }
        } finally {
            lock.unlock();
        }
    }

    private List<SenkuRunSource> validateReadyLayout() {
        if (!rootDirectory.isFileExists(SenkuFileNames.READY_FILE)) {
            throw new IndexException("Missing Senku ready marker.");
        }
        final int shardCount = SenkuMetadataCodec
                .readReadyShardCount(rootDirectory);
        final Set<String> expectedRoot = new HashSet<>();
        expectedRoot.add(SenkuFileNames.LOCK_FILE);
        expectedRoot.add(SenkuFileNames.READY_FILE);
        expectedRoot.add(SenkuFileNames.FLUSH_DIRECTORY);
        for (int shardId = 0; shardId < shardCount; shardId++) {
            expectedRoot.add(SenkuFileNames.shardDirectory(shardId));
        }
        requireExactNames(rootDirectory, expectedRoot, "ready root");
        final Directory flush = rootDirectory
                .openSubDirectory(SenkuFileNames.FLUSH_DIRECTORY);
        requireExactNames(flush, Set.of(), "ready flush directory");
        final List<SenkuRunSource> runs = new ArrayList<>(shardCount);
        for (int shardId = 0; shardId < shardCount; shardId++) {
            runs.add(readTerminalRun(shardId));
        }
        return List.copyOf(runs);
    }

    private SenkuRunSource readTerminalRun(final int shardId) {
        final Directory shard = rootDirectory.openSubDirectory(
                SenkuFileNames.shardDirectory(shardId));
        final List<String> levelNames = shard.getFileNames().toList();
        if (levelNames.size() != 1) {
            throw new IndexException(
                    "Ready shard must contain exactly one level.");
        }
        final String levelName = levelNames.get(0);
        final int level = SenkuFileNames.parseLevelDirectory(levelName);
        final Directory levelDirectory = shard.openSubDirectory(levelName);
        final List<String> runNames = levelDirectory.getFileNames().toList();
        if (runNames.size() != 1) {
            throw new IndexException(
                    "Ready shard level must contain exactly one run.");
        }
        final String runName = runNames.get(0);
        final long runId = SenkuFileNames.parseRunDirectory(runName);
        final Directory runDirectory = levelDirectory.openSubDirectory(runName);
        final SenkuRunManifest manifest = SenkuMetadataCodec
                .readRunManifest(runDirectory);
        final Set<String> expectedFiles = new HashSet<>();
        expectedFiles.add(SenkuFileNames.MANIFEST_FILE);
        for (int part = 0; part < manifest.partCount(); part++) {
            expectedFiles.add(SenkuFileNames.partFile(part));
        }
        requireExactNames(runDirectory, expectedFiles, "terminal run");
        return new SenkuRunSource(runDirectory, shardId, level, runId,
                manifest);
    }

    private List<EntryIterator<K, V>> openInputs() {
        final List<EntryIterator<K, V>> inputs = new ArrayList<>(
                terminalRuns.size());
        try {
            for (final SenkuRunSource source : terminalRuns) {
                inputs.add(source.mergeSource(dataBlockSize, 1L)
                        .open(keyTypeDescriptor, valueTypeDescriptor));
            }
            return inputs;
        } catch (Exception e) {
            closeInputs(inputs, e);
            throw e;
        }
    }

    private void closeStream(final SenkuMergedEntryIterator<K, V> iterator) {
        lock.lock();
        try {
            if (!iterator.wasClosed()) {
                iterator.close();
            }
            if (activeIterator == iterator) {
                activeIterator = null;
            }
        } finally {
            lock.unlock();
        }
    }

    private IndexException closeActiveIterator() {
        final SenkuMergedEntryIterator<K, V> iterator = activeIterator;
        activeIterator = null;
        if (iterator == null || iterator.wasClosed()) {
            return null;
        }
        try {
            iterator.close();
            return null;
        } catch (Exception e) {
            return new IndexException("Unable to close Senku ready stream.", e);
        }
    }

    private void ensureOpen() {
        if (closed || lockReleased) {
            throw new IndexException("Senku ready handle is closed.");
        }
    }

    private static void requireExactNames(final Directory directory,
            final Set<String> expected, final String description) {
        final Set<String> actual = new HashSet<>(
                directory.getFileNames().toList());
        if (!actual.equals(expected)) {
            throw new IndexException("Invalid " + description + " layout.");
        }
    }

    private static void closeInputs(
            final List<? extends EntryIterator<?, ?>> inputs,
            final Exception primary) {
        for (final EntryIterator<?, ?> input : inputs) {
            if (!input.wasClosed()) {
                try {
                    input.close();
                } catch (Exception cleanupFailure) {
                    primary.addSuppressed(cleanupFailure);
                }
            }
        }
    }

    private static IndexException appendFailure(final IndexException primary,
            final String message, final Exception failure) {
        if (primary == null) {
            return new IndexException(message, failure);
        }
        primary.addSuppressed(failure);
        return primary;
    }
}
