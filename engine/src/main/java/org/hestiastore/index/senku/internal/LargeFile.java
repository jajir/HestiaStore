package org.hestiastore.index.senku.internal;

import java.util.List;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.chunkstore.ChunkFilterSnappyCompress;
import org.hestiastore.index.chunkstore.ChunkFilterSnappyDecompress;
import org.hestiastore.index.chunkstore.ChunkStoreFile;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.Directory;

/**
 * Senku-owned logical file composed from contiguous chunk-store parts.
 */
final class LargeFile {

    private final Directory directory;
    private final DataBlockSize dataBlockSize;
    private final long maxEntriesPerPart;
    private final int partCount;

    LargeFile(final Directory directory, final DataBlockSize dataBlockSize,
            final long maxEntriesPerPart, final int partCount) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
        this.maxEntriesPerPart = Vldtn.requireGreaterThanZero(
                maxEntriesPerPart, "maxEntriesPerPart");
        this.partCount = Vldtn.requireGreaterThanOrEqualToZero(partCount,
                "partCount");
    }

    LargeFileWriterTx openWriterTx() {
        return new LargeFileWriterTx(directory, dataBlockSize,
                maxEntriesPerPart);
    }

    LargeFileReader openReader() {
        validatePartFiles();
        return new LargeFileReader(directory, dataBlockSize, partCount, null);
    }

    LargeFileReader openReader(final LargeFilePosition position) {
        final LargeFilePosition validated = Vldtn.requireNonNull(position,
                "position");
        if (validated.getPartNumber() >= partCount) {
            throw new IndexException("Large-file position part "
                    + validated.getPartNumber()
                    + " is outside manifest partCount " + partCount + ".");
        }
        validatePartFiles();
        return new LargeFileReader(directory, dataBlockSize, partCount,
                validated);
    }

    private void validatePartFiles() {
        final List<String> partNames = directory.getFileNames()
                .filter(SenkuFileNames::isPartFile).toList();
        for (final String name : partNames) {
            final int partNumber = SenkuFileNames.parsePartFile(name);
            if (partNumber >= partCount) {
                throw new IndexException(
                        "Unexpected large-file part '" + name + "'.");
            }
        }
        if (partNames.size() != partCount) {
            throw new IndexException("Large-file manifest declares " + partCount
                    + " parts but directory contains " + partNames.size()
                    + ".");
        }
    }

    static ChunkStoreFile chunkStore(final Directory directory,
            final DataBlockSize dataBlockSize, final int partNumber) {
        return chunkStore(directory, dataBlockSize,
                SenkuFileNames.partFile(partNumber));
    }

    static ChunkStoreFile chunkStore(final Directory directory,
            final DataBlockSize dataBlockSize, final String fileName) {
        return new ChunkStoreFile(directory, fileName, dataBlockSize,
                List.of(new ChunkFilterSnappyCompress(),
                        new ChunkFilterMagicNumberWriting()),
                List.of(new ChunkFilterMagicNumberValidation(),
                        new ChunkFilterSnappyDecompress()));
    }
}
