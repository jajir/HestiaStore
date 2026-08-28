package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.chunkentryfile.SingleChunkEntryIterator;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.Directory;

final class SenkuFlushTestSupport {

    private static final TypeDescriptorInteger KEYS =
            new TypeDescriptorInteger();
    private static final TypeDescriptorLong VALUES = new TypeDescriptorLong();

    private SenkuFlushTestSupport() {
        // Test utility.
    }

    /**
     * Reads one integer/long shard with the standard test descriptors.
     *
     * @param flushRoot flush root directory
     * @param generation generation identifier
     * @param shardId shard identifier
     * @param shardCount shard count
     * @param maxEntriesPerPart part rotation limit
     * @return decoded shard entries
     */
    static List<Entry<Integer, Long>> readShard(final Directory flushRoot,
            final long generation, final int shardId, final int shardCount,
            final long maxEntriesPerPart) {
        return readShard(flushRoot, generation, shardId, shardCount,
                maxEntriesPerPart, KEYS, VALUES);
    }

    /**
     * Reads one shard with caller-supplied descriptors for compatibility tests.
     *
     * @param flushRoot flush root directory
     * @param generation generation identifier
     * @param shardId shard identifier
     * @param shardCount shard count
     * @param maxEntriesPerPart part rotation limit
     * @param keyTypeDescriptor key descriptor
     * @param valueTypeDescriptor value descriptor
     * @param <K> key type
     * @param <V> value type
     * @return decoded shard entries
     */
    static <K, V> List<Entry<K, V>> readShard(final Directory flushRoot,
            final long generation, final int shardId, final int shardCount,
            final long maxEntriesPerPart,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        final Directory source = flushRoot.openSubDirectory(
                SenkuFileNames.flushDirectory(generation));
        final int partCount = SenkuMetadataCodec.readFlushPartCount(source);
        final SenkuShardIndex index = SenkuShardIndexCodec.read(source,
                DATA_BLOCK_SIZE, shardCount);
        final long recordCount = index.recordCount(shardId);
        if (recordCount == 0L) {
            return List.of();
        }
        final List<Entry<K, V>> entries = new ArrayList<>();
        final LargeFile file = new LargeFile(source, DATA_BLOCK_SIZE,
                maxEntriesPerPart, partCount);
        try (LargeFileReader pages = file.openReader(index.position(shardId))) {
            while (entries.size() < recordCount) {
                try (SingleChunkEntryIterator<K, V> page =
                        new SingleChunkEntryIterator<>(pages.read(),
                                keyTypeDescriptor, valueTypeDescriptor)) {
                    while (page.hasNext() && entries.size() < recordCount) {
                        entries.add(page.next());
                    }
                }
            }
        }
        return entries;
    }
}
