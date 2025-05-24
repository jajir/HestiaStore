package com.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.hestiastore.index.Pair;
import com.hestiastore.index.datatype.TypeDescriptorString;
import com.hestiastore.index.directory.Directory;
import com.hestiastore.index.directory.MemDirectory;
import com.hestiastore.index.segment.SegmentId;
import com.hestiastore.index.sorteddatafile.SortedDataFile;
import com.hestiastore.index.sorteddatafile.SortedDataFileWriter;

public class KeySegmentCacheSanityCheckTest {

    private final TypeDescriptorString stringTd = new TypeDescriptorString();
    private final TypeDescriptorSegmentId integerTd = new TypeDescriptorSegmentId();
    private final Directory directory = new MemDirectory();

    /**
     * Verify that loading of corrupted scarce index fails.
     * 
     * @throws Exception
     */
    @Test
    public void test_sanityCheck() throws Exception {
        final SortedDataFile<String, SegmentId> sdf = new SortedDataFile<>(
                directory, "index.map", stringTd, integerTd, 1024);

        try (SortedDataFileWriter<String, SegmentId> writer = sdf
                .openWriter()) {
            writer.write(Pair.of("aaa", SegmentId.of(1)));
            writer.write(Pair.of("bbb", SegmentId.of(2)));
            writer.write(Pair.of("ccc", SegmentId.of(3)));
            writer.write(Pair.of("ddd", SegmentId.of(4)));
            writer.write(Pair.of("eee", SegmentId.of(5)));
            writer.write(Pair.of("fff", SegmentId.of(3)));
        }

        assertThrows(IllegalStateException.class, () -> {
            try (KeySegmentCache<String> fif = new KeySegmentCache<>(directory,
                    stringTd)) {
            }
        }, "Unable to load scarce index, sanity check failed.");

    }

}
