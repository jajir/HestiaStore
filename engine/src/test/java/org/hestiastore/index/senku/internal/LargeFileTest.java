package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.hestiastore.index.senku.internal.LargeFileTestSupport.page;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class LargeFileTest {

    @Test
    void constructorRejectsInvalidDependenciesAndLimits() {
        final MemDirectory directory = new MemDirectory();

        assertThrows(IllegalArgumentException.class,
                () -> new LargeFile(null, DATA_BLOCK_SIZE, 1L, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new LargeFile(directory, null, 1L, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new LargeFile(directory, DATA_BLOCK_SIZE, 0L, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new LargeFile(directory, DATA_BLOCK_SIZE, 1L, -1));
    }

    @Test
    void openReaderRejectsMissingPart() {
        final MemDirectory directory = writeTwoParts();
        directory.deleteFile("part-00001.chunk");
        final LargeFile file = new LargeFile(directory, DATA_BLOCK_SIZE, 1L,
                2);

        assertThrows(IndexException.class, file::openReader);
    }

    @Test
    void openReaderRejectsExtraPart() {
        final MemDirectory directory = writeTwoParts();
        final LargeFile file = new LargeFile(directory, DATA_BLOCK_SIZE, 1L,
                1);

        assertThrows(IndexException.class, file::openReader);
    }

    @Test
    void positionedReaderRejectsPartOutsideManifest() {
        final MemDirectory directory = writeTwoParts();
        final LargeFile file = new LargeFile(directory, DATA_BLOCK_SIZE, 1L,
                2);
        final LargeFilePosition invalid = LargeFilePosition.of(2L, 0);

        assertThrows(IndexException.class, () -> file.openReader(invalid));
    }

    private static MemDirectory writeTwoParts() {
        final MemDirectory directory = new MemDirectory();
        final LargeFileWriterTx writer = new LargeFile(directory,
                DATA_BLOCK_SIZE, 1L, 0).openWriterTx();
        writer.appendPage(page("first"), 1);
        writer.appendPage(page("second"), 1);
        writer.commit();
        return directory;
    }
}
