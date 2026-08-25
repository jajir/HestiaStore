package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SenkuMergeSourceTest {

    private final TypeDescriptorInteger keys = new TypeDescriptorInteger();
    private final TypeDescriptorLong values = new TypeDescriptorLong();

    @Test
    void runSourceReadsCompleteRunAndEmptyRun() {
        final MemDirectory directory = new MemDirectory();
        final List<Entry<Integer, Long>> expected = List.of(Entry.of(1, 10L),
                Entry.of(2, 20L));
        final SenkuRunManifest manifest = new SenkuRunWriter<>(directory, keys,
                values, 1, 2L, DATA_BLOCK_SIZE)
                .write(new EntryIteratorList<>(expected));
        final LargeFile file = new LargeFile(directory, DATA_BLOCK_SIZE, 2L,
                manifest.partCount());

        assertEquals(expected,
                read(SenkuMergeSource.run(file, manifest.recordCount())));

        final MemDirectory emptyDirectory = new MemDirectory();
        assertFalse(SenkuMergeSource
                .run(new LargeFile(emptyDirectory, DATA_BLOCK_SIZE, 2L, 0), 0L)
                .open(keys, values).hasNext());
    }

    @Test
    void factoriesRejectInvalidRanges() {
        final LargeFile file = new LargeFile(new MemDirectory(),
                DATA_BLOCK_SIZE, 2L, 0);

        assertThrows(IllegalArgumentException.class,
                () -> SenkuMergeSource.flush(file, LargeFilePosition.of(0, 0),
                        0L));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuMergeSource.flush(file, null, 1L));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuMergeSource.run(file, -1L));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuMergeSource.run(null, 0L));
    }

    private List<Entry<Integer, Long>> read(final SenkuMergeSource source) {
        final List<Entry<Integer, Long>> entries = new ArrayList<>();
        try (EntryIterator<Integer, Long> iterator = source.open(keys, values)) {
            while (iterator.hasNext()) {
                entries.add(iterator.next());
            }
        }
        return entries;
    }
}
