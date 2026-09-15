package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.hestiastore.index.senku.internal.LargeFileTestSupport.page;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Set;
import java.util.List;
import java.util.stream.Collectors;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.chunkstore.ChunkFilterZstdCompress;
import org.hestiastore.index.chunkstore.ChunkHeader;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LargeFileWriterTxTest {

    @Test
    void commitFailureLeavesTemporaryPartAndEndsTransaction() {
        final MemDirectory failingDirectory = new MemDirectory() {
            @Override
            public void renameFile(final String currentName,
                    final String newName) {
                throw new IndexException("Injected commit failure");
            }
        };
        final LargeFileWriterTx writer = new LargeFile(failingDirectory,
                DATA_BLOCK_SIZE, 1, 0).openWriterTx();
        final ByteSequence payload = page("a");
        writer.appendPage(payload, 1);

        assertThrows(IndexException.class, writer::commit);
        assertThrows(IndexException.class, writer::commit);
        assertThrows(IndexException.class, () -> writer.appendPage(payload, 1));
        assertEquals(List.of(SenkuFileNames.temporary(SenkuFileNames.partFile(0))),
                failingDirectory.getFileNames().toList());
    }

    @Test
    void appendFailurePermanentlyEndsTransactionAfterPartRotation() {
        final LargeFileWriterTx writer = newWriter(1L);
        final ByteSequence payload = page("a");
        writer.appendPage(payload, 1);
        directory.touch(SenkuFileNames.partFile(1));

        assertThrows(IndexException.class, () -> writer.appendPage(payload, 1));
        assertThrows(IndexException.class, writer::commit);
        assertThrows(IndexException.class, () -> writer.appendPage(payload, 1));
        assertFalse(directory.isFileExists(SenkuFileNames.MANIFEST_FILE));
    }

    @Test
    void preparedAppendFailurePermanentlyEndsTransaction() {
        final LargeFileWriterTx writer = newWriter(1L);
        final ChunkData invalid = ChunkData.ofSequence(0, 0, 0,
                SenkuStorageFormat.createDefault().keyCodec().getId(), page("a"));

        assertThrows(IndexException.class,
                () -> writer.appendPreparedPage(invalid, 1));
        assertThrows(IndexException.class, writer::commit);
        assertThrows(IndexException.class,
                () -> writer.appendPreparedPage(invalid, 1));
        assertFalse(directory.isFileExists(SenkuFileNames.partFile(0)));
    }

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
        final LargeFilePosition exactBoundary = writer.appendPage(page("b"), 1);
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

    @Test
    void preparedPagesRotateRoundTripAndObeyTransactionLifecycle() {
        final LargeFileWriterTx writer = newWriter(3L);
        final ByteSequence payload = page("a".repeat(1000));
        final ChunkData prepared = new ChunkFilterMagicNumberWriting()
                .apply(new ChunkFilterZstdCompress(3)
                        .apply(ChunkData.ofSequence(0, 0,
                                ChunkHeader.MAGIC_NUMBER, SenkuStorageFormat
                                        .createDefault().keyCodec().getId(),
                                payload)));
        assertEquals(0, writer.appendPreparedPage(prepared, 2).getPartNumber());
        assertEquals(1, writer.appendPreparedPage(prepared, 2).getPartNumber());
        assertEquals(2, writer.commit());
        try (LargeFileReader reader = new LargeFile(directory, DATA_BLOCK_SIZE,
                3L, 2).openReader()) {
            assertArrayEquals(payload.toByteArray(),
                    reader.read().toByteArray());
            assertArrayEquals(payload.toByteArray(),
                    reader.read().toByteArray());
            assertEquals(null, reader.read());
        }
        assertThrows(IndexException.class,
                () -> writer.appendPreparedPage(prepared, 1));
    }

    @Test
    void preparedPageRejectsWrongCodecAndFailedPartCanBeAborted() {
        final LargeFileWriterTx writer = newWriter(3L);
        final ChunkData valid = ChunkData.ofSequence(0, 0,
                ChunkHeader.MAGIC_NUMBER,
                SenkuStorageFormat.createDefault().keyCodec().getId(),
                page("a"));
        final ChunkData wrongCodec = valid.withVersion(99);
        assertThrows(IllegalArgumentException.class,
                () -> writer.appendPreparedPage(wrongCodec, 1));
        assertThrows(IllegalArgumentException.class,
                () -> writer.appendPreparedPage(valid, 4));
        directory.touch("part-00000.chunk");
        final IndexException failure = assertThrows(IndexException.class,
                () -> writer.appendPreparedPage(valid, 1));
        writer.abort(failure);
        assertThrows(IndexException.class, writer::commit);
    }

    private LargeFileWriterTx newWriter(final long maxEntriesPerPart) {
        return new LargeFile(directory, DATA_BLOCK_SIZE, maxEntriesPerPart, 0)
                .openWriterTx();
    }

    private Set<String> fileNames() {
        return directory.getFileNames().collect(Collectors.toSet());
    }
}
