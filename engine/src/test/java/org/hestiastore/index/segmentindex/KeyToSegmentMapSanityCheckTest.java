package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.sorteddatafile.SortedDataFile;
import org.junit.jupiter.api.Test;

class KeyToSegmentMapSanityCheckTest {

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
        final SortedDataFile<String, SegmentId> sdf = SortedDataFile
                .fromDirectory(
                        directory,
                        "index.map", stringTd, integerTd, 1024);

        sdf.openWriterTx().execute(writer -> {
            writer.write(Entry.of("aaa", SegmentId.of(1)));
            writer.write(Entry.of("bbb", SegmentId.of(2)));
            writer.write(Entry.of("ccc", SegmentId.of(3)));
            writer.write(Entry.of("ddd", SegmentId.of(4)));
            writer.write(Entry.of("eee", SegmentId.of(5)));
            writer.write(Entry.of("fff", SegmentId.of(3)));
        });
        assertThrows(IllegalStateException.class, () -> {
            try (KeyToSegmentMap<String> fif = new KeyToSegmentMap<>(
                    directory,
                    stringTd)) {
                // Intentionally left empty to trigger Exception
            }
        }, "Unable to load scarce index, sanity check failed.");

    }

}
