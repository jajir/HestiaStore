package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.chunkentryfile.SingleChunkEntryIterator;
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

    static List<Entry<Integer, Long>> readShard(final Directory flushRoot,
            final long generation, final int shardId, final int shardCount,
            final long maxEntriesPerPart) {
        final Directory source = flushRoot.openSubDirectory(
                SenkuFileNames.flushDirectory(generation));
        final int partCount = SenkuMetadataCodec.readFlushPartCount(source);
        final SenkuShardIndex index = SenkuShardIndexCodec.read(source,
                DATA_BLOCK_SIZE, shardCount);
        final long recordCount = index.recordCount(shardId);
        if (recordCount == 0L) {
            return List.of();
        }
        final List<Entry<Integer, Long>> entries = new ArrayList<>();
        final LargeFile file = new LargeFile(source, DATA_BLOCK_SIZE,
                maxEntriesPerPart, partCount);
        try (LargeFileReader pages = file.openReader(index.position(shardId))) {
            while (entries.size() < recordCount) {
                try (SingleChunkEntryIterator<Integer, Long> page =
                        new SingleChunkEntryIterator<>(pages.read(), KEYS,
                                VALUES)) {
                    while (page.hasNext() && entries.size() < recordCount) {
                        entries.add(page.next());
                    }
                }
            }
        }
        return entries;
    }
}
