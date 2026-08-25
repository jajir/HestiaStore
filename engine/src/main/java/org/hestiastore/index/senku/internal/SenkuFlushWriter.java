package org.hestiastore.index.senku.internal;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.function.ToIntFunction;

import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkentryfile.SingleChunkEntryWriterImpl;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;

/**
 * Sorts and publishes one immutable flush generation.
 */
final class SenkuFlushWriter<K, V> {

    private final Directory flushDirectory;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final ToIntFunction<K> shardHashFunction;
    private final Comparator<Map.Entry<K, V>> entryComparator;
    private final int shardCount;
    private final int maxKeysPerPage;
    private final long maxEntriesPerPart;
    private final DataBlockSize dataBlockSize;

    SenkuFlushWriter(final Directory flushDirectory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final ToIntFunction<K> shardHashFunction, final int shardCount,
            final int maxKeysPerPage, final long maxEntriesPerPart,
            final DataBlockSize dataBlockSize) {
        this.flushDirectory = Vldtn.requireNonNull(flushDirectory,
                "flushDirectory");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.shardHashFunction = Vldtn.requireNonNull(shardHashFunction,
                "shardHashFunction");
        final Comparator<K> keyComparator = keyTypeDescriptor.getComparator();
        this.entryComparator = (first, second) -> keyComparator
                .compare(first.getKey(), second.getKey());
        this.shardCount = Vldtn.requireGreaterThanZero(shardCount,
                "shardCount");
        this.maxKeysPerPage = Vldtn.requireGreaterThanZero(maxKeysPerPage,
                "maxKeysPerPage");
        this.maxEntriesPerPart = Vldtn.requireGreaterThanZero(
                maxEntriesPerPart, "maxEntriesPerPart");
        Vldtn.requireTrue(maxEntriesPerPart >= maxKeysPerPage,
                "maxEntriesPerPart must be greater than or equal to maxKeysPerPage");
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
    }

    void write(final long generation, final Map<K, V> entries) {
        Vldtn.requireGreaterThanOrEqualToZero(generation, "generation");
        final Map<K, V> validatedEntries = Vldtn.requireNonNull(entries,
                "entries");
        Vldtn.requireTrue(!validatedEntries.isEmpty(),
                "Property 'entries' must not be empty");
        final String directoryName = SenkuFileNames.flushDirectory(generation);
        if (flushDirectory.isFileExists(directoryName)) {
            throw new IndexException(
                    "Flush generation '" + directoryName + "' already exists.");
        }
        flushDirectory.mkdir(directoryName);
        final Directory generationDirectory = flushDirectory
                .openSubDirectory(directoryName);
        try {
            writeGeneration(generationDirectory, validatedEntries);
        } catch (IndexException e) {
            throw e;
        } catch (Exception e) {
            throw new IndexException(
                    "Unable to write flush generation '" + directoryName
                            + "'.",
                    e);
        }
    }

    private void writeGeneration(final Directory generationDirectory,
            final Map<K, V> entries) {
        final int[] counts = countShards(entries);
        final int[] starts = starts(counts);
        final Map.Entry<K, V>[] ordered = orderByShard(entries, starts);
        final LargeFile largeFile = new LargeFile(generationDirectory,
                dataBlockSize, maxEntriesPerPart, 0);
        final LargeFileWriterTx writer = largeFile.openWriterTx();
        try {
            final long[] packedPositions = new long[shardCount];
            final long[] recordCounts = new long[shardCount];
            for (int shardId = 0; shardId < shardCount; shardId++) {
                final int from = starts[shardId];
                final int count = counts[shardId];
                final int to = from + count;
                recordCounts[shardId] = count;
                if (count == 0) {
                    continue;
                }
                Arrays.sort(ordered, from, to, entryComparator);
                packedPositions[shardId] = writeShard(writer, ordered, from, to)
                        .getPacked();
            }
            final int partCount = writer.commit();
            SenkuShardIndexCodec.write(generationDirectory, dataBlockSize,
                    new SenkuShardIndex(packedPositions, recordCounts));
            SenkuMetadataCodec.publishFlushManifest(generationDirectory,
                    partCount);
        } catch (Exception e) {
            writer.abort(e);
            throw e;
        }
    }

    private LargeFilePosition writeShard(final LargeFileWriterTx writer,
            final Map.Entry<K, V>[] entries, final int from, final int to) {
        LargeFilePosition firstPosition = null;
        int pageStart = from;
        while (pageStart < to) {
            final int remaining = to - pageStart;
            final int pageEnd = remaining <= maxKeysPerPage ? to
                    : pageStart + maxKeysPerPage;
            final SingleChunkEntryWriterImpl<K, V> pageWriter =
                    new SingleChunkEntryWriterImpl<>(keyTypeDescriptor,
                            valueTypeDescriptor);
            for (int index = pageStart; index < pageEnd; index++) {
                final Map.Entry<K, V> entry = entries[index];
                pageWriter.put(Entry.of(entry.getKey(), entry.getValue()));
            }
            final LargeFilePosition position = writer.appendPage(
                    pageWriter.closeSequence(), pageEnd - pageStart);
            if (firstPosition == null) {
                firstPosition = position;
            }
            pageStart = pageEnd;
        }
        return Vldtn.requireNonNull(firstPosition, "firstPosition");
    }

    private int[] countShards(final Map<K, V> entries) {
        final int[] counts = new int[shardCount];
        for (final K key : entries.keySet()) {
            counts[shardId(key)]++;
        }
        return counts;
    }

    private int[] starts(final int[] counts) {
        final int[] starts = new int[shardCount];
        int next = 0;
        for (int shardId = 0; shardId < shardCount; shardId++) {
            starts[shardId] = next;
            next = Math.addExact(next, counts[shardId]);
        }
        return starts;
    }

    @SuppressWarnings("unchecked")
    private Map.Entry<K, V>[] orderByShard(final Map<K, V> entries,
            final int[] starts) {
        final Map.Entry<K, V>[] ordered = new Map.Entry[entries.size()];
        final int[] next = Arrays.copyOf(starts, starts.length);
        for (final Map.Entry<K, V> entry : entries.entrySet()) {
            final int shardId = shardId(entry.getKey());
            ordered[next[shardId]++] = entry;
        }
        return ordered;
    }

    private int shardId(final K key) {
        return Math.floorMod(shardHashFunction.applyAsInt(key), shardCount);
    }
}
