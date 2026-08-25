package org.hestiastore.index.senku.internal;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.chunkstore.CellPosition;
import org.hestiastore.index.chunkstore.ChunkStoreFile;
import org.hestiastore.index.chunkstore.ChunkStoreWriter;
import org.hestiastore.index.chunkstore.ChunkStoreWriterTx;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.Directory;

/**
 * One-shot append transaction for a Senku large file.
 */
final class LargeFileWriterTx {

    private static final int SENKU_PAGE_VERSION = 1;

    private final Directory directory;
    private final DataBlockSize dataBlockSize;
    private final long maxEntriesPerPart;

    private ChunkStoreWriterTx currentTransaction;
    private ChunkStoreWriter currentWriter;
    private int currentPartNumber = -1;
    private int partCount;
    private long entriesInCurrentPart;
    private boolean committed;

    LargeFileWriterTx(final Directory directory,
            final DataBlockSize dataBlockSize,
            final long maxEntriesPerPart) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
        this.maxEntriesPerPart = Vldtn.requireGreaterThanZero(
                maxEntriesPerPart, "maxEntriesPerPart");
    }

    LargeFilePosition appendPage(final ByteSequence page,
            final int entryCount) {
        ensureOpen();
        final ByteSequence validatedPage = Vldtn.requireNonNull(page, "page");
        Vldtn.requireTrue(!validatedPage.isEmpty(),
                "Property 'page' must not be empty");
        final int validatedCount = Vldtn.requireGreaterThanZero(entryCount,
                "entryCount");
        Vldtn.requireTrue(validatedCount <= maxEntriesPerPart,
                "Property 'entryCount' must not exceed maxEntriesPerPart");
        try {
            if (mustRotate(validatedCount)) {
                commitCurrentPart();
            }
            if (currentWriter == null) {
                openNextPart();
            }
            final CellPosition position = currentWriter
                    .writeSequence(validatedPage, SENKU_PAGE_VERSION);
            entriesInCurrentPart += validatedCount;
            return LargeFilePosition.of(currentPartNumber,
                    position.getValue());
        } catch (Exception e) {
            closeAfterFailure(e);
            throw asIndexException("Unable to append large-file page.", e);
        }
    }

    int commit() {
        ensureOpen();
        committed = true;
        if (currentWriter == null) {
            return 0;
        }
        try {
            commitCurrentPart();
            return partCount;
        } catch (Exception e) {
            closeAfterFailure(e);
            throw asIndexException("Unable to commit large file.", e);
        }
    }

    void abort(final Exception primary) {
        Vldtn.requireNonNull(primary, "primary");
        committed = true;
        closeAfterFailure(primary);
    }

    private boolean mustRotate(final int nextEntryCount) {
        return currentWriter != null
                && nextEntryCount > maxEntriesPerPart - entriesInCurrentPart;
    }

    private void openNextPart() {
        if (partCount == Integer.MAX_VALUE) {
            throw new IndexException(
                    "Large file cannot contain more than Integer.MAX_VALUE parts.");
        }
        currentPartNumber = partCount;
        final String partName = SenkuFileNames.partFile(currentPartNumber);
        requireAbsent(partName);
        requireAbsent(SenkuFileNames.temporary(partName));
        final ChunkStoreFile chunkStore = LargeFile.chunkStore(directory,
                dataBlockSize, currentPartNumber);
        currentTransaction = chunkStore.openWriteTx();
        currentWriter = currentTransaction.open();
        entriesInCurrentPart = 0L;
        partCount++;
    }

    private void commitCurrentPart() {
        final ChunkStoreWriter writer = currentWriter;
        final ChunkStoreWriterTx transaction = currentTransaction;
        currentWriter = null;
        currentTransaction = null;
        writer.close();
        transaction.commit();
    }

    private void closeAfterFailure(final Exception primary) {
        final ChunkStoreWriter writer = currentWriter;
        currentWriter = null;
        currentTransaction = null;
        if (writer == null || writer.wasClosed()) {
            return;
        }
        try {
            writer.close();
        } catch (Exception cleanupFailure) {
            primary.addSuppressed(cleanupFailure);
        }
    }

    private void requireAbsent(final String fileName) {
        if (directory.isFileExists(fileName)) {
            throw new IndexException(
                    "Large-file part '" + fileName + "' already exists.");
        }
    }

    private void ensureOpen() {
        if (committed) {
            throw new IndexException("Large-file writer is already committed.");
        }
    }

    private static IndexException asIndexException(final String message,
            final Exception cause) {
        if (cause instanceof IndexException) {
            return (IndexException) cause;
        }
        return new IndexException(message, cause);
    }
}
