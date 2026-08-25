package org.hestiastore.index.senku.internal;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Reader;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.chunkstore.CellPosition;
import org.hestiastore.index.chunkstore.Chunk;
import org.hestiastore.index.chunkstore.ChunkStoreReader;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.Directory;

/**
 * Sequential page reader that crosses Senku large-file parts transparently.
 */
final class LargeFileReader implements Reader<ByteSequence>, AutoCloseable {

    private static final int SENKU_PAGE_VERSION = 1;

    private final Directory directory;
    private final DataBlockSize dataBlockSize;
    private final int partCount;
    private final int firstPartNumber;
    private final int firstLocalPosition;

    private int partNumber;
    private ChunkStoreReader currentReader;
    private boolean eof;
    private boolean closed;

    LargeFileReader(final Directory directory,
            final DataBlockSize dataBlockSize, final int partCount,
            final LargeFilePosition startPosition) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
        this.partCount = Vldtn.requireGreaterThanOrEqualToZero(partCount,
                "partCount");
        if (startPosition == null) {
            partNumber = 0;
            firstPartNumber = 0;
            firstLocalPosition = 0;
        } else {
            Vldtn.requireTrue(startPosition.getPartNumber() < partCount,
                    "Large-file start position must be inside partCount");
            partNumber = (int) startPosition.getPartNumber();
            firstPartNumber = partNumber;
            firstLocalPosition = startPosition.getLocalPosition();
        }
        eof = partNumber >= partCount;
    }

    @Override
    public ByteSequence read() {
        ensureOpen();
        if (eof) {
            return null;
        }
        try {
            while (partNumber < partCount) {
                openReaderIfNeeded();
                final Chunk chunk = currentReader.read();
                if (chunk != null) {
                    validateVersion(chunk);
                    return chunk.getPayloadSequence();
                }
                closeCurrentReader();
                partNumber++;
            }
            eof = true;
            return null;
        } catch (Exception e) {
            closeAfterFailure(e);
            if (e instanceof IndexException) {
                throw (IndexException) e;
            }
            throw new IndexException("Unable to read large-file page.", e);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        closeCurrentReader();
    }

    private void openReaderIfNeeded() {
        if (currentReader != null) {
            return;
        }
        final CellPosition position = CellPosition.of(dataBlockSize,
                partNumber == firstPartNumber ? firstLocalPosition : 0);
        currentReader = LargeFile.chunkStore(directory, dataBlockSize,
                partNumber).openReader(position);
    }

    private static void validateVersion(final Chunk chunk) {
        final int version = chunk.getHeader().getVersion();
        if (version != SENKU_PAGE_VERSION) {
            throw new IndexException("Unexpected Senku page version " + version
                    + "; expected " + SENKU_PAGE_VERSION + ".");
        }
    }

    private void closeCurrentReader() {
        final ChunkStoreReader reader = currentReader;
        currentReader = null;
        if (reader != null && !reader.wasClosed()) {
            reader.close();
        }
    }

    private void closeAfterFailure(final Exception primary) {
        closed = true;
        try {
            closeCurrentReader();
        } catch (Exception cleanupFailure) {
            primary.addSuppressed(cleanupFailure);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IndexException("Large-file reader is closed.");
        }
    }
}
