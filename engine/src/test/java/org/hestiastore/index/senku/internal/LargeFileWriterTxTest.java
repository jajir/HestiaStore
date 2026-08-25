package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.hestiastore.index.senku.internal.LargeFileTestSupport.page;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;
import java.util.stream.Collectors;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LargeFileWriterTxTest {

    private MemDirectory directory;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
    }

    @Test
    void commitWithoutPageCreatesNoPart() {
        final LargeFileWriterTx writer = newWriter(3L);

        assertEquals(0, writer.commit());
        assertEquals(Set.of(), fileNames());
    }

    @Test
    void appendPageReturnsPartAndLocalPosition() {
        final LargeFileWriterTx writer = newWriter(3L);

        final LargeFilePosition first = writer.appendPage(page("first"), 1);
        final LargeFilePosition second = writer.appendPage(page("second"), 1);

        assertEquals(0L, first.getPartNumber());
        assertEquals(0, first.getLocalPosition());
        assertEquals(0L, second.getPartNumber());
        assertEquals(1, writer.commit());
        assertEquals(Set.of("part-00000.chunk"), fileNames());
    }

    @Test
    void appendPageRotatesOnlyWhenNextPageWouldExceedEntryLimit() {
        final LargeFileWriterTx writer = newWriter(3L);

        final LargeFilePosition first = writer.appendPage(page("a"), 2);
        final LargeFilePosition exactBoundary = writer.appendPage(page("b"),
                1);
        final LargeFilePosition rotated = writer.appendPage(page("c"), 1);

        assertEquals(0L, first.getPartNumber());
        assertEquals(0L, exactBoundary.getPartNumber());
        assertEquals(1L, rotated.getPartNumber());
        assertEquals(2, writer.commit());
        assertEquals(Set.of("part-00000.chunk", "part-00001.chunk"),
                fileNames());
    }

    @Test
    void appendPageRejectsInvalidPayloadAndCount() {
        final LargeFileWriterTx writer = newWriter(3L);

        assertThrows(IllegalArgumentException.class,
                () -> writer.appendPage(null, 1));
        assertThrows(IllegalArgumentException.class,
                () -> writer.appendPage(ByteSequence.EMPTY, 1));
        assertThrows(IllegalArgumentException.class,
                () -> writer.appendPage(page("a"), 0));
        assertThrows(IllegalArgumentException.class,
                () -> writer.appendPage(page("a"), 4));
    }

    @Test
    void writerRejectsAppendAndCommitAfterCommit() {
        final LargeFileWriterTx writer = newWriter(3L);
        writer.appendPage(page("a"), 1);
        writer.commit();

        assertThrows(IndexException.class,
                () -> writer.appendPage(page("b"), 1));
        assertThrows(IndexException.class, writer::commit);
    }

    @Test
    void writerRejectsExistingCommittedOrTemporaryPart() {
        directory.touch("part-00000.chunk");
        assertThrows(IndexException.class,
                () -> newWriter(3L).appendPage(page("a"), 1));

        final MemDirectory temporaryDirectory = new MemDirectory();
        temporaryDirectory.touch("part-00000.chunk.tmp");
        final LargeFileWriterTx temporaryWriter = new LargeFile(
                temporaryDirectory, DATA_BLOCK_SIZE, 3L, 0).openWriterTx();
        assertThrows(IndexException.class,
                () -> temporaryWriter.appendPage(page("a"), 1));
    }

    private LargeFileWriterTx newWriter(final long maxEntriesPerPart) {
        return new LargeFile(directory, DATA_BLOCK_SIZE, maxEntriesPerPart, 0)
                .openWriterTx();
    }

    private Set<String> fileNames() {
        return directory.getFileNames().collect(Collectors.toSet());
    }
}
