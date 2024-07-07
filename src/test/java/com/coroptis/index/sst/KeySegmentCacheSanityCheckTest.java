package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.segment.SegmentId;
import com.coroptis.index.sstfile.SstFile;
import com.coroptis.index.sstfile.SstFileWriter;

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
        final SstFile<String, SegmentId> sdf = new SstFile<>(directory,
                "index.map", integerTd.getTypeWriter(),
                integerTd.getTypeReader(), stringTd.getComparator(),
                stringTd.getConvertorFromBytes(),
                stringTd.getConvertorToBytes(),1024);

        try (SstFileWriter<String, SegmentId> writer = sdf.openWriter()) {
            writer.put(Pair.of("aaa", SegmentId.of(1)));
            writer.put(Pair.of("bbb", SegmentId.of(2)));
            writer.put(Pair.of("ccc", SegmentId.of(3)));
            writer.put(Pair.of("ddd", SegmentId.of(4)));
            writer.put(Pair.of("eee", SegmentId.of(5)));
            writer.put(Pair.of("fff", SegmentId.of(3)));
        }

        assertThrows(IllegalStateException.class, () -> {
            try (KeySegmentCache<String> fif = new KeySegmentCache<>(directory,
                    stringTd)) {
            }
        }, "Unable to load scarce index, sanity check failed.");

    }

}
