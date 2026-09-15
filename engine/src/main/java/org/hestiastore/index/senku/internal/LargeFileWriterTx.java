package org.hestiastore.index.senku.internal;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.chunkstore.CellPosition;
import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.chunkstore.ChunkStoreFile;
import org.hestiastore.index.chunkstore.ChunkStoreWriter;
import org.hestiastore.index.chunkstore.ChunkStoreWriterTx;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.Directory;

/**
 * One-shot append transaction for a Senku large file.
 */
final class LargeFileWriterTx {

    private final SenkuStorageFormat format;

    private final Directory directory;
    private final DataBlockSize dataBlockSize;
    private final long maxEntriesPerPart;

    private ChunkStoreWriterTx currentTransaction;
    private ChunkStoreWriter currentWriter;
    private int currentPartNumber = -1;
    private int partCount;
    private long entriesInCurrentPart;
    private boolean finished;

    /** Creates a one-shot transaction with the default storage format. */
    LargeFileWriterTx(final Directory directory,
            final DataBlockSize dataBlockSize, final long maxEntriesPerPart) {
        this(directory, dataBlockSize, maxEntriesPerPart,
                SenkuStorageFormat.createDefault());
    }

    /** Creates a transaction using the index's immutable storage format. */
    LargeFileWriterTx(final Directory directory,
            final DataBlockSize dataBlockSize, final long maxEntriesPerPart,
            final SenkuStorageFormat format) {
        this.format = Vldtn.requireNonNull(format, "format");
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
        this.maxEntriesPerPart = Vldtn.requireGreaterThanZero(maxEntriesPerPart,
                "maxEntriesPerPart");
    }

    /**
     * Encodes and appends one complete page, rotating parts at entry limits.
     * An I/O or encoding failure permanently ends this transaction; partially
     * written parts remain unpublished for the owner's cleanup.
     */
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
                    .writeSequence(validatedPage, format.keyCodec().getId());
            entriesInCurrentPart += validatedCount;
            return LargeFilePosition.of(currentPartNumber, position.getValue());
        } catch (Exception e) {
            closeAfterFailure(e);
            throw asIndexException("Unable to append large-file page.", e);
        }
    }

    /**
     * Appends a page already encoded with this index's immutable compression
     * and key codec. This is the sole ordered append owner; page-preparation
     * workers never access the transaction or its part writer.
     */
    LargeFilePosition appendPreparedPage(final ChunkData page,
            final int entryCount) {
        ensureOpen();
        final ChunkData validated = Vldtn.requireNonNull(page, "page");
        Vldtn.requireTrue(!validated.getPayloadSequence().isEmpty(),
                "Prepared page must not be empty");
        Vldtn.requireTrue(validated.getVersion() == format.keyCodec().getId(),
                "Prepared page key codec does not match the index");
        Vldtn.requireGreaterThanZero(entryCount, "entryCount");
        Vldtn.requireTrue(entryCount <= maxEntriesPerPart,
                "entryCount must not exceed maxEntriesPerPart");
        try {
            if (mustRotate(entryCount)) {
                commitCurrentPart();
            }
            if (currentWriter == null) {
                openNextPart();
            }
            final CellPosition position = currentWriter
                    .writePreparedChunk(validated);
            entriesInCurrentPart += entryCount;
            return LargeFilePosition.of(currentPartNumber, position.getValue());
        } catch (Exception e) {
            closeAfterFailure(e);
            throw asIndexException("Unable to append prepared large-file page.",
                    e);
        }
    }

    /** Commits every appended part once and returns the physical part count. */
    int commit() {
        ensureOpen();
        finished = true;
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

    /** Ends the transaction and attaches cleanup failures to the cause. */
    void abort(final Exception primary) {
        Vldtn.requireNonNull(primary, "primary");
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
                dataBlockSize, partName, format.compression());
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
        finished = true;
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
        if (finished) {
            throw new IndexException("Large-file writer is already finished.");
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
