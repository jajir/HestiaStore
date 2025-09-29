package org.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.Pair;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.sorteddatafile.SortedDataFile;
import org.junit.jupiter.api.Test;

class KeySegmentCacheSanityCheckTest {

    private final TypeDescriptorShortString stringTd = new TypeDescriptorShortString();
    private final TypeDescriptorSegmentId integerTd = new TypeDescriptorSegmentId();
    private final Directory directory = new MemDirectory();

    /**
     * Verify that loading of corrupted scarce index fails.
     * 
     * @
     */
    @Test
    void test_sanityCheck() {
        final SortedDataFile<String, SegmentId> sdf = new SortedDataFile<>(
                directory, "index.map", stringTd, integerTd, 1024);

        sdf.openWriterTx().execute(writer -> {
            writer.put(Pair.of("aaa", SegmentId.of(1)));
            writer.put(Pair.of("bbb", SegmentId.of(2)));
            writer.put(Pair.of("ccc", SegmentId.of(3)));
            writer.put(Pair.of("ddd", SegmentId.of(4)));
            writer.put(Pair.of("eee", SegmentId.of(5)));
            writer.put(Pair.of("fff", SegmentId.of(3)));
        });
        assertThrows(IllegalStateException.class, () -> {
            try (KeySegmentCache<String> fif = new KeySegmentCache<>(directory,
                    stringTd)) {
                // Intentionally left empty to trigger Exception
            }
        }, "Unable to load scarce index, sanity check failed.");

    }

}
