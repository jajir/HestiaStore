package com.coroptis.index.simpledatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.coroptis.index.Pair;
import com.coroptis.index.PairReader;
import com.coroptis.index.PairWriter;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FsDirectory;

public class SimpleDataFileIntegerationTest {

    private final TypeDescriptorInteger intTd = new TypeDescriptorInteger();
    private final TypeDescriptorString stringTd = new TypeDescriptorString();

    @TempDir
    Path tempDir;

    /*
     * This is strange. This test write data that read one part add additional
     * data to file and continue in reading. Surprisingly data are read as
     * nothing changed.
     */
    private final static int keys1 = 1000 * 1;
    private final static int keys2 = 1000 * 1;
    private final static int moreKeys = 1777;

    @Test
    void test_execure_compat_during_readng() throws Exception {
        final Directory directory = new FsDirectory(tempDir.toFile());
        final SimpleDataFile<Integer, String> sdf = new SimpleDataFile<>(
                directory, "sdf", intTd, stringTd, (k, v1, v2) -> v2);

        // write keys
        try (final PairWriter<Integer, String> writer = sdf.openCacheWriter()) {
            for (int i = 0; i < keys1 + keys2; i++) {
                writer.put(Pair.of(i, STR2));
            }
        }
        sdf.compact();

        try (final PairReader<Integer, String> reader = sdf.openReader()) {
            for (int i = 0; i < keys1; i++) {
                assertEquals(i, reader.read().getKey());
            }

            try (final PairWriter<Integer, String> writer = sdf
                    .openCacheWriter()) {
                for (int i = 0; i < moreKeys; i++) {
                    writer.put(Pair.of(-i, STR2));
                }
            }
            sdf.compact();

            for (int i = 0; i < keys2; i++) {
                assertEquals(keys1 + i, reader.read().getKey());
            }
            assertNull(reader.read());
        }

    }

    private final static String STR = "0123456789a";
    // 100 znaku
    private final static String STR2 = STR + STR + STR + STR + STR + STR + STR
            + STR + STR + STR + "fasd";

}
