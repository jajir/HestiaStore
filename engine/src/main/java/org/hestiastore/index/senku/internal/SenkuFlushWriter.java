package org.hestiastore.index.senku.internal;

import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkentryfile.SingleChunkEntryWriterImpl;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.Directory;

/**
 * Sorts and publishes one immutable flush generation.
 */
final class SenkuFlushWriter<K, V> {

    private static final int PARALLEL_SORT_MIN_ENTRIES = 8_192;
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
    private static final int MAX_LONG_KEY_BYTES = 2 + Long.BYTES;

    private final SenkuStorageFormat format;
    private final Directory flushDirectory;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final ToIntFunction<K> shardHashFunction;
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
        this(flushDirectory, keyTypeDescriptor, valueTypeDescriptor,
                shardHashFunction, shardCount, maxKeysPerPage,
                maxEntriesPerPart, dataBlockSize,
                SenkuStorageFormat.createDefault());
    }

    /** Creates this component with the index's immutable storage format. */
    SenkuFlushWriter(final Directory flushDirectory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final ToIntFunction<K> shardHashFunction, final int shardCount,
            final int maxKeysPerPage, final long maxEntriesPerPart,
            final DataBlockSize dataBlockSize,
            final SenkuStorageFormat format) {
        this.format = Vldtn.requireNonNull(format, "format");
        this.flushDirectory = Vldtn.requireNonNull(flushDirectory,
                "flushDirectory");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.shardHashFunction = Vldtn.requireNonNull(shardHashFunction,
                "shardHashFunction");
        this.shardCount = Vldtn.requireGreaterThanZero(shardCount,
                "shardCount");
        this.maxKeysPerPage = Vldtn.requireGreaterThanZero(maxKeysPerPage,
                "maxKeysPerPage");
        this.maxEntriesPerPart = Vldtn.requireGreaterThanZero(maxEntriesPerPart,
                "maxEntriesPerPart");
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
    void write(final long generation, final List<? extends Map<K, V>> entries) {
        Vldtn.requireGreaterThanOrEqualToZero(generation, "generation");
        final List<? extends Map<K, V>> validatedEntries = Vldtn
                .requireNonNull(entries, "entries");
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
            writeGeneration(generationDirectory, validatedEntries, entryCount);
        } catch (IndexException e) {
            throw e;
        } catch (Exception e) {
            throw new IndexException(
                    "Unable to write flush generation '" + directoryName + "'.",
                    e);
        }
    }

    private void writeGeneration(final Directory generationDirectory,
            final List<? extends Map<K, V>> entries, final int entryCount) {
        final int[] counts = countShards(entries);
        final int[] starts = starts(counts);
        final SenkuFlushOrder<K, V> ordered = orderByShard(entries, starts,
                entryCount);
        sortShards(ordered, starts, counts);
        final LargeFile largeFile = new LargeFile(generationDirectory,
                dataBlockSize, maxEntriesPerPart, 0, format);
        final LargeFileWriterTx writer = largeFile.openWriterTx();
        try {
            final long[] recordCounts = new long[shardCount];
            for (int shardId = 0; shardId < shardCount; shardId++) {
                recordCounts[shardId] = counts[shardId];
            }
            final long[] packedPositions;
            final int recordBytes = fixedRecordBytes();
            if (entryCount >= PARALLEL_SORT_MIN_ENTRIES && recordBytes > 0) {
                packedPositions = new SenkuFlushPagePipeline<>(ordered,
                        keyTypeDescriptor, valueTypeDescriptor, format,
                        recordBytes, maxKeysPerPage)
                        .write(writer, starts, counts);
            } else {
                packedPositions = new long[shardCount];
                for (int shardId = 0; shardId < shardCount; shardId++) {
                    final int from = starts[shardId];
                    final int count = counts[shardId];
                    final int to = from + count;
                    if (count == 0) {
                        continue;
                    }
                    packedPositions[shardId] = writeShard(writer, ordered, from,
                            to).getPacked();
                }
            }
            final int partCount = writer.commit();
            SenkuShardIndexCodec.write(generationDirectory, dataBlockSize,
                    new SenkuShardIndex(packedPositions, recordCounts),
                    format.compression());
            SenkuMetadataCodec.publishFlushManifest(generationDirectory,
                    partCount);
        } catch (Exception e) {
            writer.abort(e);
            throw e;
        }
    }

    private int fixedRecordBytes() {
        if (keyTypeDescriptor.getClass() != TypeDescriptorLong.class) {
            return 0;
        }
        if (valueTypeDescriptor.getClass() == TypeDescriptorNull.class) {
            return MAX_LONG_KEY_BYTES;
        }
        return valueTypeDescriptor.getClass() == TypeDescriptorLong.class
                ? MAX_LONG_KEY_BYTES + Long.BYTES
                : 0;
    }

    /**
     * Sorts independent persistent-shard ranges concurrently before sequential
     * page encoding. Each task owns a disjoint array range.
     */
    private void sortShards(final SenkuFlushOrder<K, V> ordered,
            final int[] starts, final int[] counts) {
        SenkuFlushSortTask.sortAll(ordered, starts, counts,
                entryCount(counts) >= PARALLEL_SORT_MIN_ENTRIES);
    }

    private LargeFilePosition writeShard(final LargeFileWriterTx writer,
            final SenkuFlushOrder<K, V> entries, final int from, final int to) {
        LargeFilePosition firstPosition = null;
        int pageStart = from;
        while (pageStart < to) {
            final int remaining = to - pageStart;
            final int pageEnd = remaining <= maxKeysPerPage ? to
                    : pageStart + maxKeysPerPage;
            final SingleChunkEntryWriterImpl<K, V> pageWriter = newPageWriter(
                    pageEnd - pageStart);
            for (int index = pageStart; index < pageEnd; index++) {
                if (entries.hasPrimitiveLongKeys()) {
                    pageWriter.putLongKey(entries.longKey(index),
                            entries.value(index));
                } else {
                    pageWriter.put(entries.key(index), entries.value(index));
                }
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

    private SingleChunkEntryWriterImpl<K, V> newPageWriter(
            final int recordCount) {
        if (keyTypeDescriptor.getClass() != TypeDescriptorLong.class) {
            return new SingleChunkEntryWriterImpl<>(keyTypeDescriptor,
                    valueTypeDescriptor, MAX_ARRAY_SIZE, format.keyCodec());
        }
        final int maxRecordBytes;
        if (valueTypeDescriptor.getClass() == TypeDescriptorNull.class) {
            maxRecordBytes = MAX_LONG_KEY_BYTES;
        } else if (valueTypeDescriptor.getClass() == TypeDescriptorLong.class) {
            maxRecordBytes = MAX_LONG_KEY_BYTES + Long.BYTES;
        } else {
            return new SingleChunkEntryWriterImpl<>(keyTypeDescriptor,
                    valueTypeDescriptor, MAX_ARRAY_SIZE, format.keyCodec());
        }
        final int maxEncodedBytes = (int) Math.min(MAX_ARRAY_SIZE,
                (long) recordCount * maxRecordBytes);
        return new SingleChunkEntryWriterImpl<>(keyTypeDescriptor,
                valueTypeDescriptor, maxEncodedBytes, format.keyCodec());
    }

    private int[] countShards(final List<? extends Map<K, V>> entries) {
        final int[] counts = new int[shardCount];
        for (final Map<K, V> stripe : entries) {
            if (stripe instanceof SenkuIngestionMap<?, ?> map
                    && map.isLongSet()) {
                map.forEachLongWithHash((key,
                        hash) -> counts[Math.floorMod(hash, shardCount)]++);
            } else {
                stripe.forEach((key, value) -> counts[shardId(key)]++);
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

    private SenkuFlushOrder<K, V> orderByShard(
            final List<? extends Map<K, V>> entries, final int[] starts,
            final int entryCount) {
        final SenkuFlushOrder<K, V> ordered = new SenkuFlushOrder<>(
                keyTypeDescriptor, valueTypeDescriptor, entryCount);
        final int[] next = starts.clone();
        for (final Map<K, V> stripe : entries) {
            if (stripe instanceof SenkuIngestionMap<?, ?> map
                    && map.isLongSet()) {
                map.forEachLongWithHash((key, hash) -> ordered
                        .setLong(next[Math.floorMod(hash, shardCount)]++, key));
            } else {
                stripe.forEach((key, value) -> {
                    final int shardId = shardId(key);
                    ordered.set(next[shardId]++, key, value);
                });
            }
        }
        return ordered;
    }

    private int entryCount(final int[] counts) {
        int count = 0;
        for (final int shardEntryCount : counts) {
            count = Math.addExact(count, shardEntryCount);
        }
        return count;
    }

    private int entryCount(final List<? extends Map<K, V>> entries) {
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
