package org.hestiastore.index.senku.internal;

import java.util.List;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkentryfile.KeyPageCodec;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.chunkstore.ChunkFilterZstdCompress;
import org.hestiastore.index.chunkstore.Compression;
import org.hestiastore.index.chunkstore.ChunkFilterZstdDecompress;
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
    private final SenkuStorageFormat format;

    LargeFile(final Directory directory, final DataBlockSize dataBlockSize,
            final long maxEntriesPerPart, final int partCount) {
        this(directory, dataBlockSize, maxEntriesPerPart, partCount,
                SenkuStorageFormat.createDefault());
    }

    /** Creates a large-file accessor with explicit output encoding. */
    LargeFile(final Directory directory, final DataBlockSize dataBlockSize,
            final long maxEntriesPerPart, final int partCount,
            final SenkuStorageFormat format) {
        this.format = Vldtn.requireNonNull(format, "format");
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
        this.maxEntriesPerPart = Vldtn.requireGreaterThanZero(maxEntriesPerPart,
                "maxEntriesPerPart");
        this.partCount = Vldtn.requireGreaterThanOrEqualToZero(partCount,
                "partCount");
    }

    LargeFileWriterTx openWriterTx() {
        return new LargeFileWriterTx(directory, dataBlockSize,
                maxEntriesPerPart, format);
    }

    LargeFileReader openReader() {
        return openReaderInternal(null, fixedWeightCodecOrNull());
    }

    LargeFileReader openReader(final LargeFilePosition position) {
        return openReaderInternal(Vldtn.requireNonNull(position, "position"),
                fixedWeightCodecOrNull());
    }

    /**
     * Opens a range using the complete immutable codec from index metadata;
     * null position starts at the beginning. Page identifiers are validated
     * before exposing payloads, including during primitive merge reads.
     */
    LargeFileReader openReaderWithCodec(final LargeFilePosition position,
            final KeyPageCodec<?> codec) {
        return openReaderInternal(position,
                Vldtn.requireNonNull(codec, "keyPageCodec"));
    }

    private KeyPageCodec<?> fixedWeightCodecOrNull() {
        return format.keyCodec().isLongFixedWeightDeltaVarint()
                ? format.keyCodec()
                : null;
    }

    private LargeFileReader openReaderInternal(final LargeFilePosition position,
            final KeyPageCodec<?> codec) {
        if (position != null && position.getPartNumber() >= partCount) {
            throw new IndexException("Large-file position part "
                    + position.getPartNumber()
                    + " is outside manifest partCount " + partCount + ".");
        }
        validatePartFiles();
        return new LargeFileReader(directory, dataBlockSize, partCount,
                position, codec);
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
        return chunkStore(directory, dataBlockSize, fileName,
                Compression.zstd(3));
    }

    /** Creates a chunk store whose reader recognizes raw and Zstd pages. */
    static ChunkStoreFile chunkStore(final Directory directory,
            final DataBlockSize dataBlockSize, final String fileName,
            final Compression compression) {
        return new ChunkStoreFile(directory, fileName, dataBlockSize,
                compression.getLevel() == 0
                        ? List.of(new ChunkFilterMagicNumberWriting())
                        : List.of(
                                new ChunkFilterZstdCompress(
                                        compression.getLevel()),
                                new ChunkFilterMagicNumberWriting()),
                List.of(new ChunkFilterMagicNumberValidation(),
                        new ChunkFilterZstdDecompress()));
    }
}
