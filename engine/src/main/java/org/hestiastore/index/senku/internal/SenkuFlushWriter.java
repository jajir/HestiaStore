package org.hestiastore.index.senku.internal;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

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

    private static final int PARALLEL_SORT_MIN_ENTRIES = 8_192;

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

    /**
     * Writes one detached striped batch without copying its mappings into an
     * aggregate map. The caller owns the maps and keeps them immutable until
     * this method returns.
     *
     * @param generation generation identifier
     * @param entries    non-empty aggregate of detached mutation stripes
     */
    void write(final long generation, final List<Map<K, V>> entries) {
        Vldtn.requireGreaterThanOrEqualToZero(generation, "generation");
        final List<Map<K, V>> validatedEntries = Vldtn.requireNonNull(entries,
                "entries");
        final int entryCount = entryCount(validatedEntries);
        Vldtn.requireTrue(entryCount > 0,
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
            writeGeneration(generationDirectory, validatedEntries,
                    entryCount);
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
            final List<Map<K, V>> entries, final int entryCount) {
        final int[] counts = countShards(entries);
        final int[] starts = starts(counts);
        final Map.Entry<K, V>[] ordered = orderByShard(entries, starts,
                entryCount);
        sortShards(ordered, starts, counts);
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

    /**
     * Sorts independent persistent-shard ranges concurrently before sequential
     * page encoding. Each task owns a disjoint array range.
     */
    private void sortShards(final Map.Entry<K, V>[] ordered,
            final int[] starts, final int[] counts) {
        IntStream shards = IntStream.range(0, shardCount);
        if (ordered.length >= PARALLEL_SORT_MIN_ENTRIES && shardCount > 1
                && ForkJoinPool.getCommonPoolParallelism() > 1) {
            shards = shards.parallel();
        }
        shards.forEach(shardId -> {
            final int from = starts[shardId];
            Arrays.sort(ordered, from, from + counts[shardId], entryComparator);
        });
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
                pageWriter.put(entry.getKey(), entry.getValue());
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

    private int[] countShards(final List<Map<K, V>> entries) {
        final int[] counts = new int[shardCount];
        for (final Map<K, V> stripe : entries) {
            for (final K key : stripe.keySet()) {
                counts[shardId(key)]++;
            }
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
    private Map.Entry<K, V>[] orderByShard(final List<Map<K, V>> entries,
            final int[] starts, final int entryCount) {
        final Map.Entry<K, V>[] ordered = new Map.Entry[entryCount];
        final int[] next = Arrays.copyOf(starts, starts.length);
        for (final Map<K, V> stripe : entries) {
            for (final Map.Entry<K, V> entry : stripe.entrySet()) {
                final int shardId = shardId(entry.getKey());
                ordered[next[shardId]++] = entry;
            }
        }
        return ordered;
    }

    private int entryCount(final List<Map<K, V>> entries) {
        int count = 0;
        for (int index = 0; index < entries.size(); index++) {
            final Map<K, V> map = Vldtn.requireNonNull(entries.get(index),
                    "entries[" + index + "]");
            count = Math.addExact(count, map.size());
        }
        return count;
    }

    private int shardId(final K key) {
        return Math.floorMod(shardHashFunction.applyAsInt(key), shardCount);
    }
}
