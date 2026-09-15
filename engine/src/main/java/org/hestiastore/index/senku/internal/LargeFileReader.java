package org.hestiastore.index.senku.internal;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Reader;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.chunkstore.CellPosition;
import org.hestiastore.index.chunkentryfile.KeyPageCodec;
import org.hestiastore.index.chunkentryfile.KeyPageCodecs;
import org.hestiastore.index.chunkstore.Chunk;
import org.hestiastore.index.chunkstore.ChunkStoreReader;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.Directory;

/**
 * Sequential page reader that crosses Senku large-file parts transparently.
 */
final class LargeFileReader implements Reader<ByteSequence>, AutoCloseable {

    private KeyPageCodec<?> keyCodec;
    private final KeyPageCodec<?> requiredCodec;

    /** Returns the validated codec of the most recently read page. */
    @SuppressWarnings("unchecked")
    <K> KeyPageCodec<K> keyCodec() {
        return (KeyPageCodec<K>) Vldtn.requireNonNull(keyCodec,
                "currentPageCodec");
    }

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
            final LargeFilePosition startPosition,
            final KeyPageCodec<?> requiredCodec) {
        this.requiredCodec = requiredCodec;
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
                    keyCodec = resolveCodec(chunk.getHeader().getVersion());
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

    private KeyPageCodec<?> resolveCodec(final int id) {
        if (requiredCodec == null) {
            return KeyPageCodecs.fromId(id);
        }
        if (requiredCodec.getId() != id) {
            throw new IndexException(
                    "Page codec does not match Senku root metadata.");
        }
        return requiredCodec;
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
        currentReader = LargeFile
                .chunkStore(directory, dataBlockSize, partNumber)
                .openReader(position);
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
